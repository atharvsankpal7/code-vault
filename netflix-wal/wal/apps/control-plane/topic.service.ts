import { Admin } from "@platformatic/kafka";
import {
  TKafkaTopicMapResponse,
  TopicDetails,
  TopicOperationType,
} from "@wal/config";
import db from "./control-plane-db";
import { kafkaTopic } from "./control-plane-db/schema";
import { inArray } from "drizzle-orm";

export const getTopicMap = async (
  admin: Admin,
): Promise<TKafkaTopicMapResponse> => {
  const topicNameList = await admin.listTopics();
  const topicMap = new Map<string, TopicDetails>();

  if (topicNameList.length === 0) {
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
    topicMap.set(topic.topicName, {
      operationType: topic.operationType as TopicOperationType,
      workerWaitTimeInMinutes: topic.workerWaitTimeInMinutes,
      acknowledgement: topic.minInsyncReplicas, // -1 in database = all in kafka
    });
  }

  return topicMap;
};
