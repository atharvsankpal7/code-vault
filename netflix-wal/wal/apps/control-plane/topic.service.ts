import { Admin } from "@platformatic/kafka";
import {
  TKafkaConsumerTopicMapResponse,
  TKafkaProducerTopicMapResponse,
  TopicOperationType,
} from "@wal/config";
import db from "./control-plane-db";
import {
  deliveryTarget,
  kafkaTopic,
  targetTopicSubscription,
} from "./control-plane-db/schema";
import { eq, inArray } from "drizzle-orm";
import { createLogger } from "@wal/logger";

const log = createLogger("control-plane:topic-service");

export const getTopicMap = async (
  admin: Admin,
): Promise<TKafkaProducerTopicMapResponse> => {
  const topicNameList = await admin.listTopics();
  const topicMap: TKafkaProducerTopicMapResponse = {};

  if (topicNameList.length === 0) {
    log.warn("No topics found in kafka, returning empty topic map");
    return topicMap;
  }

  // Only topics that exist in kafka are advertised to producers, so a topic the
  // reconsiler has not created yet is never handed out.
  const topicDetailsFromDb = await db
    .select({
      topicName: kafkaTopic.kafka_topic_name,
      operationType: kafkaTopic.topic_production_type,
      workerWaitTimeInMinutes: kafkaTopic.worker_wait_time_in_minutes,
      minInsyncReplicas: kafkaTopic.min_insync_replicas,
    })
    .from(kafkaTopic)
    .where(inArray(kafkaTopic.kafka_topic_name, topicNameList));

  for (const topic of topicDetailsFromDb) {
    topicMap[topic.topicName] = {
      operationType: topic.operationType as TopicOperationType,
      workerWaitTimeInMinutes: topic.workerWaitTimeInMinutes,
      acknowledgement: topic.minInsyncReplicas, // -1 in database = all in kafka
    };
  }

  log.debug(`Built topic map for ${topicDetailsFromDb.length} topics`);
  return topicMap;
};

export const getConsumerTopicMap = async (
  admin: Admin,
  topicName?: string,
): Promise<TKafkaConsumerTopicMapResponse> => {
  const topicMap: TKafkaConsumerTopicMapResponse = {};
  let topicList = await admin.listTopics();
  if (topicName) {
    if (!topicList.includes(topicName)) {
      throw new Error("Given topic not present in the kafka");
    }
    topicList = [topicName];
  }

  const rows = await db
    .select({
      targetName: deliveryTarget.target_name,
      communication_type: deliveryTarget.endpoint_communication_type,
      endpoint: deliveryTarget.endpoint,
      timeout: deliveryTarget.timeout,
      topic: targetTopicSubscription.kafka_topic_name,
    })
    .from(targetTopicSubscription)
    .innerJoin(
      deliveryTarget,
      eq(targetTopicSubscription.delivery_target_id, deliveryTarget.id),
    )
    .where(inArray(targetTopicSubscription.kafka_topic_name, topicList));

  for (const { targetName, topic, ...targetDetails } of rows) {
    const existing = topicMap[targetName];
    if (existing) {
      existing.topics.push(topic);
      continue;
    }
    topicMap[targetName] = { ...targetDetails, topics: [topic] };
  }

  return topicMap;
};
