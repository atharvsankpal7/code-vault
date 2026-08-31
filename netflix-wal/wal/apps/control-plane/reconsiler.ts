import { ConfigResourceTypes, Kafka } from "kafkajs";
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

  const kafkaTopicList = await admin.listTopics();
  const dbTopicList = await db
    .select({
      topicName: kafkaTopic.kafka_topic_name,
      numPartitions: kafkaTopic.partition_count,
      replicationFactor: kafkaTopic.replication_factor,
      minInsyncReplicas: kafkaTopic.min_insync_replicas,
      updatedAt: kafkaTopic.updated_at,
      lastReconciledAt: kafkaTopic.last_reconciled_at,
    })
    .from(kafkaTopic);

  const kafkaTopics = new Set(kafkaTopicList);
  const missingTopics = dbTopicList.filter(
    ({ topicName }) => !kafkaTopics.has(topicName),
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

    await db
      .update(kafkaTopic)
      .set({ reconciliation_status: "done", last_reconciled_at: new Date() })
      .where(
        inArray(
          kafkaTopic.kafka_topic_name,
          missingTopics.map(({ topicName }) => topicName),
        ),
      );
  }
  // Reconcile existing topics whose desired configuration has changed since the
  // last successful reconciliation.
  const existingTopics = dbTopicList.filter(({ topicName }) =>
    kafkaTopics.has(topicName),
  );
  const topicsNeedingConfigUpdate = existingTopics.filter(
    ({ lastReconciledAt, updatedAt }) =>
      lastReconciledAt === null || lastReconciledAt < updatedAt,
  );

  if (topicsNeedingConfigUpdate.length !== 0) {
    await admin.alterConfigs({
      validateOnly: false,
      resources: topicsNeedingConfigUpdate.map(
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

    const reconciledAt = new Date();
    await db
      .update(kafkaTopic)
      .set({ last_reconciled_at: reconciledAt, reconciliation_status: "done" })
      .where(
        and(
          inArray(
            kafkaTopic.kafka_topic_name,
            topicsNeedingConfigUpdate.map(({ topicName }) => topicName),
          ),
          or(
            isNull(kafkaTopic.last_reconciled_at),
            lt(kafkaTopic.last_reconciled_at, kafkaTopic.updated_at),
          ),
        ),
      );
  }

  return missingTopics.length !== 0 || topicsNeedingConfigUpdate.length !== 0;
};
