import { Producer, stringSerializers } from "@platformatic/kafka";
import { createLogger } from "@wal/logger";
import Config from "./config";
import { boss, WAL_OUTBOX_QUEUE } from "./pg-boss";
import { KAKFA_CONFIG } from "./kafka-config";

const log = createLogger("producer:wal");

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
    log.warn(`Rejected WAL request for unknown topic ${topicName}`);
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
    log.info(`Message sent to Kafka topic ${topicName}`);
  } else if (operationType === "database") {
    const { workerWaitTimeInMinutes } = topicDetails;
    await boss.send(
      WAL_OUTBOX_QUEUE,
      { topic_name: topicName, message },
      { expireInSeconds: workerWaitTimeInMinutes * 60 },
    );
    log.info(`Message queued in outbox for topic ${topicName}`);
  }
  return true;
};
