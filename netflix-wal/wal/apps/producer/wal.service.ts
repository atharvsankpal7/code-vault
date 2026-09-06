import db from "@wal/wal-db";
import { Producer, stringSerializers } from "@platformatic/kafka";
import Config from "./config";
import { wal_outbox } from "@wal/wal-db/schema";
import { KAKFA_CONFIG } from ".";
interface IGenerateWalRequest {
  topicName: string;
  message: string;
}
export const generateWal = async ({
  topicName,
  message,
}: IGenerateWalRequest) => {
  const topicDetails = KAKFA_CONFIG.get(topicName);
  if (!topicDetails) return false;
  const { operationType } = topicDetails;

  if (operationType === "kafka") {
    const producer = new Producer({
      clientId: Config.clientId,
      bootstrapBrokers: Config.kafkaBrokers.split(","),
      serializers: stringSerializers,
    });

    const result = await producer.send({
      messages: [
        {
          topic: topicName,
          value: message,
        },
      ],
    });
  } else if (operationType === "database") {
    await db
      .insert(wal_outbox)
      .values({ topic_name: topicName, message, status: "pending" });
  }
  return true;
};
