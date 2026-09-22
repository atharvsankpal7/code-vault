import { Admin, ConfigResourceTypes } from "@platformatic/kafka";
import { eq, inArray, sql } from "drizzle-orm";
import Config from "./config";
import db from "./control-plane-db";
import { kafkaTopic } from "./control-plane-db/schema";

export const reconsiler = async (admin: Admin) => {
  const [kafkaTopicList, dbTopicList] = await Promise.all([
    admin.listTopics(),
    db
      .select({
        id: kafkaTopic.id,
        topicName: kafkaTopic.kafka_topic_name,
        numPartitions: kafkaTopic.partition_count,
        replicationFactor: kafkaTopic.replication_factor,
        minInsyncReplicas: kafkaTopic.min_insync_replicas,
        updatedAt: kafkaTopic.updated_at,
        version: kafkaTopic.version,
        reconciled_version: kafkaTopic.reconciled_version,
      })
      .from(kafkaTopic),
  ]);

  const existingKafkaTopics = new Set(kafkaTopicList);
  const missingTopics = dbTopicList.filter(
    ({ topicName }) => !existingKafkaTopics.has(topicName),
  );

  if (missingTopics.length !== 0) {
    await db
      .update(kafkaTopic)
      .set({ reconciliation_status: "inprogress" })
      .where(
        inArray(
          kafkaTopic.kafka_topic_name,
          missingTopics.map(({ topicName }) => topicName),
        ),
      );

    for (const t of missingTopics) {
      await admin.createTopics({
        topics: [t.topicName],
        partitions: t.numPartitions,
        replicas: t.replicationFactor,
        configs: [
          {
            name: "min.insync.replicas",
            value: String(t.minInsyncReplicas),
          },
        ],
      });

      // Stamp the version from the snapshot we acted on, not kafkaTopic.version.
      // A concurrent bump then leaves reconciled_version < version, so the next
      // run picks the topic up instead of silently reporting it as converged.
      // Writing per topic also keeps already-created topics out of "inprogress"
      // when a later topic in the loop fails.
      await db
        .update(kafkaTopic)
        .set({
          reconciliation_status: "done",
          last_reconciled_at: new Date(),
          reconciled_version: t.version,
        })
        .where(eq(kafkaTopic.id, t.id));
    }
  }
  // Reconcile existing topics whose desired configuration has changed since the
  // last successful reconciliation.
  const topicsOutOfVersion = dbTopicList.filter(
    ({ topicName, version, reconciled_version }) =>
      existingKafkaTopics.has(topicName) &&
      (reconciled_version === null || reconciled_version < version),
  );
  if (topicsOutOfVersion.length === 0) {
    console.log("no topics out of version");
    return missingTopics.length !== 0;
  }
  const [topicMetadataLista, kafkaTopicReplicas] = await Promise.all([
    admin.metadata({
      topics: topicsOutOfVersion.map((t) => t.topicName),
    }),
    admin.describeConfigs({
      includeSynonyms: false,
      resources: topicsOutOfVersion.map((t) => ({
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: t.topicName,
        configurationKeys: ["min.insync.replicas"], // strict names to avoid overhead
      })),
    }),
  ]);

  const outOf_MinISR_Topics = topicsOutOfVersion.filter((t) => {
    const topic = kafkaTopicReplicas.find(
      (topic) => topic.resourceName === t.topicName,
    );
    const minIsrConfig = topic?.configs?.find(
      (entry) => entry.name === "min.insync.replicas",
    );

    if (!minIsrConfig?.value) {
      return false;
    }

    return Number(minIsrConfig.value) !== t.minInsyncReplicas;
  });

  if (topicsOutOfVersion.length !== 0) {
    await admin.alterConfigs({
      resources: outOf_MinISR_Topics.map(
        ({ topicName, minInsyncReplicas }) => ({
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: topicName,
          configs: [
            {
              name: "min.insync.replicas",
              value: String(minInsyncReplicas),
            },
          ],
        }),
      ),
    });

    const outOfPartitionOrderTopics = topicsOutOfVersion.filter((t) => {
      const topic = topicMetadataLista.topics.get(t.topicName);
      return topic?.partitionsCount && topic?.partitionsCount < t.numPartitions;
    });

    if (outOfPartitionOrderTopics.length > 0) {
      await admin.createPartitions({
        topics: outOfPartitionOrderTopics.map((t) => ({
          name: t.topicName,
          count: t.numPartitions,
        })),
        validateOnly: false,
      });
    }

    // One round trip, but each row needs its own version, which a shared SET
    // clause cannot express. The versions ride in as a VALUES list joined on
    // topic name. Casts are explicit because Postgres types bare VALUES
    // literals as `unknown`/text in this join position.
    const desiredVersions = sql.join(
      topicsOutOfVersion.map(
        (t) => sql`(${t.topicName}::text, ${t.version}::integer)`,
      ),
      sql`, `,
    );

    // `reconciled_version < v.version` replaces the old last_reconciled_at vs
    // updated_at predicate: updated_at only moves via drizzle's $onUpdateFn, so
    // a version bumped by raw SQL never matched it. Comparing versions also acts
    // as a compare-and-swap, so a concurrent run that already stamped a higher
    // version is never regressed.
    await db.execute(sql`
      update ${kafkaTopic}
      set reconciled_version = v.version,
          reconciliation_status = 'done',
          last_reconciled_at = now()
      from (values ${desiredVersions}) as v(topic_name, version)
      where ${kafkaTopic.kafka_topic_name} = v.topic_name
        and ${kafkaTopic.reconciled_version} < v.version
    `);
  }

  return missingTopics.length !== 0 || topicsOutOfVersion.length !== 0;
};
