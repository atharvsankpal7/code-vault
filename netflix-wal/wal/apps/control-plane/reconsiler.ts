import {
  ConfigResourceTypes,
  ITopicConfig,
  ITopicPartitionConfig,
  Kafka,
} from "kafkajs";
import { and, inArray, isNull, lt, or } from "drizzle-orm";
import Config from "./config";
import db from "./control-plane-db";
import { kafkaTopic } from "./control-plane-db/schema";

const kafka = new Kafka({
  clientId: Config.kafkaClientId,
  brokers: Config.kafkaBrokers.split(","),
});

const admin = kafka.admin();

export const reconsiler = async () => {
  await admin.connect();
  const [kafkaTopicList, dbTopicList] = await Promise.all([
    await admin.listTopics(),
    await db
      .select({
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

    await admin.createTopics({
      waitForLeaders: true,
      topics: missingTopics.map(
        ({
          topicName,
          numPartitions,
          replicationFactor,
          minInsyncReplicas,
        }) => ({
          topic: topicName,
          numPartitions,
          replicationFactor,
          configEntries: [
            {
              name: "min.insync.replicas",
              value: String(minInsyncReplicas),
            },
          ],
        }),
      ),
    });
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
      existingKafkaTopics.has(topicName) && reconciled_version < version,
  );

  const kafkaTopicReplicas = await admin.describeConfigs({
    includeSynonyms: false,
    resources: topicsOutOfVersion.map((t) => ({
      type: ConfigResourceTypes.TOPIC,
      name: t.topicName,
      configNames: ["min.insync.replicas"], // Explicit filter lowers payload size
    })),
  });

  const outOfReplicationOrderTopics = topicsOutOfVersion.filter((t) => {
    const topic = kafkaTopicReplicas.resources.find(
      (topic) => topic.resourceName === t.topicName,
    );
    return (
      topic?.configEntries &&
      Number(topic?.configEntries[0].configValue) < t.minInsyncReplicas &&
      t.replicationFactor >= t.minInsyncReplicas
    );
  });

  if (topicsOutOfVersion.length !== 0) {
    await admin.alterConfigs({
      validateOnly: false,
      resources: outOfReplicationOrderTopics.map(
        ({ topicName, minInsyncReplicas }) => ({
          type: ConfigResourceTypes.TOPIC,
          name: topicName,
          configEntries: [
            {
              name: "min.insync.replicas",
              value: String(minInsyncReplicas),
            },
          ],
        }),
      ),
    });

    const topicMetaDataList = await admin.fetchTopicMetadata({
      topics: topicsOutOfVersion.map((topic) => topic.topicName),
    });
    const outOfPartitionOrderTopics = topicsOutOfVersion.filter((t) => {
      const topic = topicMetaDataList.topics.find(
        (topic) => topic.name === t.topicName,
      );
      return topic?.partitions && topic?.partitions.length < t.numPartitions;
    });

    if (outOfPartitionOrderTopics.length > 0) {
      await admin.createPartitions({
        topicPartitions: outOfPartitionOrderTopics.map((t) => ({
          topic: t.topicName,
          count: t.numPartitions,
        })),
        validateOnly: false,
      });
    }

    const reconciledAt = new Date();
    await db
      .update(kafkaTopic)
      .set({ last_reconciled_at: reconciledAt, reconciliation_status: "done" })
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
