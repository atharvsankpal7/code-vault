import { Kafka } from "kafkajs";
import { inArray } from "drizzle-orm";
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
      .set({ reconciliation_status: "done" })
      .where(
        inArray(
          kafkaTopic.kafka_topic_name,
          missingTopics.map(({ topicName }) => topicName),
        ),
      );
  }
  // get the topics who were not newly created
  // update the config settings where the last_reconsiled_at is less than updated_at and update that toic in kafka update both to have same time

};
