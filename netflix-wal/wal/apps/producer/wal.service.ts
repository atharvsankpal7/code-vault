import { Producer, stringSerializers } from "@platformatic/kafka";
import Config from "./config";
import { boss, WAL_OUTBOX_QUEUE } from "./pg-boss";
import { KAKFA_CONFIG } from "./kafka-config";
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
  const topicDetails = KAKFA_CONFIG[topicName];
  if (!topicDetails) {
    throw new Error(`Topic details not found for topic ${topicName}`);
  }
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
    const { workerWaitTimeInMinutes } = topicDetails;
    await boss.send(
      WAL_OUTBOX_QUEUE,
      { topic_name: topicName, message },
      { expireInSeconds: workerWaitTimeInMinutes * 60 },
    );
  }
  return true;
};
