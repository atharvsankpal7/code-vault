import db from "@wal/wal-db";
import { Producer, stringSerializers } from "@platformatic/kafka";
import Config from "./config";
import { wal } from "@wal/wal-db/schema";
interface IGenerateWalRequest {
  topicName: string;
  message: string;
  type: "kafka" | "database";
}
export const generateWal = async ({
  topicName,
  message,
  type,
}: IGenerateWalRequest) => {
  if (type === "kafka") {
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
  } else if (type === "database") {
    await db.insert(wal).values({ topic_name: topicName, message });
  }
};
