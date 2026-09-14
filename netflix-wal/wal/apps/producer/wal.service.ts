import { Producer, stringSerializers } from "@platformatic/kafka";
import Config from "./config";
import { KAKFA_CONFIG } from ".";
import { boss, WAL_OUTBOX_QUEUE } from "./pg-boss";
interface IGenerateWalRequest {
  topicName: string;
  message: string;
}
export const kafkaProducer = new Producer({
  clientId: Config.clientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
  serializers: stringSerializers,
});

export const generateWal = async ({
  topicName,
  message,
}: IGenerateWalRequest) => {
  const topicDetails = KAKFA_CONFIG.get(topicName);
  if (!topicDetails) return false;
  const { operationType } = topicDetails;

  if (operationType === "kafka") {
    await kafkaProducer.send({
      messages: [
        {
          topic: topicName,
          value: message,
        },
      ],
    });
    console.log(`Message sent to Kafka topic ${topicName}:`);
  } else if (operationType === "database") {
    await boss.send(WAL_OUTBOX_QUEUE, { topic_name: topicName, message });
  }
  return true;
};
