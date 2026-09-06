import { Admin, ConfigResourceTypes } from "@platformatic/kafka";
import { TKafkaTopicMapResponse, TopicDetails } from "@wal/config";
import db from "./control-plane-db";
import { kafkaTopic } from "./control-plane-db/schema";
import { inArray } from "drizzle-orm";
// export interface TopicDetails {
//   operationType: TopicOperationType;
//   workerWaitTimeInMinutes: number;
//   acknowledgement: number;
// }
export const getTopicMap = async (
  admin: Admin,
): Promise<TKafkaTopicMapResponse> => {
  const topicNameList = await admin.listTopics();
  const topicMap = new Map<string, TopicDetails>();

  const [topicMetadataLista, kafkaTopicReplicas, topicDetailsFromDb] =
    await Promise.all([
      admin.metadata({
        topics: topicNameList.map((t) => t),
      }),
      admin.describeConfigs({
        includeSynonyms: false,
        resources: topicNameList.map((t) => ({
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: t,
          configurationKeys: ["min.insync.replicas"], // strict names to avoid overhead
        })),
      }),
      db
        .select()
        .from(kafkaTopic)
        .where(inArray(kafkaTopic.kafka_topic_name, topicNameList)),
    ]);

  //create a map and put value as json of TopicDetails type kafkaTopic.min_insync_replicas is value of the acknowledgement field

  return topicMap;
};
