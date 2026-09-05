import db from "@wal/wal-db";
import { Producer, stringSerializers } from "@platformatic/kafka";
import Config from "./config";
import { wal } from "@wal/wal-db/schema";
interface IGenerateWalRequest {
  topicName: string;
  message: string;
  type: "kafka" | "database";
}
export const sendMessageToKafka = async ({
  topicName,
  message,
}: IGenerateWalRequest) => {
  const producer = new Producer({
    clientId: Config.clientId,
    bootstrapBrokers: Config.kafkaBrokers.split(","),
    serializers: stringSerializers,
  });

  // perform the computation that we would want to perform and convert the raw message into something that our kafka should strore
};
