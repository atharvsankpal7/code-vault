import { Admin, ConfigResourceTypes } from "@platformatic/kafka";
import { and, inArray, isNull, lt, or } from "drizzle-orm";
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

    //note: batch if parallel topic creation becomes an issue
    await Promise.all(
      missingTopics.map((t) =>
        admin.createTopics({
          topics: [t.topicName],
          partitions: t.numPartitions,
          replicas: t.replicationFactor,
          configs: [
            {
              name: "min.insync.replicas",
              value: String(t.minInsyncReplicas),
            },
          ],
        }),
      ),
    );

    const currentTime = new Date();
    await db
      .update(kafkaTopic)
      .set({
        reconciliation_status: "done",
        last_reconciled_at: currentTime,
        reconciled_version: kafkaTopic.version,
      })
      .where(
        inArray(
          kafkaTopic.kafka_topic_name,
          missingTopics.map(({ topicName }) => topicName),
        ),
      );
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

    const reconciledAt = new Date();
    await db
      .update(kafkaTopic)
      .set({
        last_reconciled_at: reconciledAt,
        reconciliation_status: "done",
        reconciled_version: kafkaTopic.version,
      })
      .where(
        and(
          inArray(
            kafkaTopic.kafka_topic_name,
            topicsOutOfVersion.map(({ topicName }) => topicName),
          ),
          or(
            isNull(kafkaTopic.last_reconciled_at),
            lt(kafkaTopic.last_reconciled_at, kafkaTopic.updated_at),
          ),
        ),
      );
  }

  return missingTopics.length !== 0 || topicsOutOfVersion.length !== 0;
};
