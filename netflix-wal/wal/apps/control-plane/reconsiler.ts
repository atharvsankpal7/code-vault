import { Admin, ConfigResourceTypes } from "@platformatic/kafka";
import { createLogger } from "@wal/logger";
import { eq, inArray, sql } from "drizzle-orm";
import Config from "./config";
import db from "./control-plane-db";
import { kafkaTopic } from "./control-plane-db/schema";

const log = createLogger("control-plane:reconciler");

// Runs as one transaction holding FOR UPDATE locks on every topic row, so
// concurrent reconciles and topic patches wait for it instead of racing it.
export const reconsiler = async (admin: Admin) =>
  db.transaction(async (tx) => {
    const [kafkaTopicList, dbTopicList] = await Promise.all([
      admin.listTopics(),
      tx
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
        .from(kafkaTopic)
        .for("update"),
    ]);

    log.debug(
      `Reconciling ${dbTopicList.length} db topics against ${kafkaTopicList.length} kafka topics`,
    );
    const existingKafkaTopics = new Set(kafkaTopicList);
    const missingTopics = dbTopicList.filter(
      ({ topicName }) => !existingKafkaTopics.has(topicName),
    );

    if (missingTopics.length !== 0) {
      log.info(`Creating ${missingTopics.length} missing topics in kafka`);
      await tx
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

        await tx
          .update(kafkaTopic)
          .set({
            reconciliation_status: "done",
            last_reconciled_at: new Date(),
            reconciled_version: t.version,
          })
          .where(eq(kafkaTopic.id, t.id));
        log.info(`Created topic ${t.topicName} at version ${t.version}`);
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
      log.info("no topics out of version");
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
      log.info(
        `Reconciling ${topicsOutOfVersion.length} out-of-version topics, ${outOf_MinISR_Topics.length} need min.insync.replicas update`,
      );
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
        return (
          topic?.partitionsCount && topic?.partitionsCount < t.numPartitions
        );
      });

      if (outOfPartitionOrderTopics.length > 0) {
        log.info(
          `Increasing partitions for ${outOfPartitionOrderTopics.map((t) => t.topicName).join(", ")}`,
        );
        await admin.createPartitions({
          topics: outOfPartitionOrderTopics.map((t) => ({
            name: t.topicName,
            count: t.numPartitions,
          })),
          validateOnly: false,
        });
      }

      const desiredVersions = sql.join(
        topicsOutOfVersion.map(
          (t) => sql`(${t.topicName}::text, ${t.version}::integer)`,
        ),
        sql`, `,
      );

      await tx.execute(sql`
      update ${kafkaTopic}
      set reconciled_version = v.version,
          reconciliation_status = 'done',
          last_reconciled_at = now()
      from (values ${desiredVersions}) as v(topic_name, version)
      where ${kafkaTopic.kafka_topic_name} = v.topic_name
        and ${kafkaTopic.reconciled_version} < v.version
    `);
      log.info("Reconciled versions updated in database");
    }

    return missingTopics.length !== 0 || topicsOutOfVersion.length !== 0;
  });
