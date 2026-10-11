// tbh this is an app in itself
// we are using kafkajs cause we want pause/resume functionality on the topic which Platformatic currently doesn't provide, check. https://github.com/platformatic/kafka/issues/128. maybe i'll create a PR to solve this issue in the future by talking with some of the folks at platformatic.
import "dotenv/config";
import { IHeaders, Kafka } from "kafkajs";
import { uuidv7 } from "uuidv7";
import { validateConfig } from "@wal/config";
import { createLogger } from "@wal/logger";
import Config, { RETRY_TOPIC_NAMES } from "./config";
import { KAKFA_CONFIG } from "./infra-config";
import { sendMessageToTarget } from "./consumer.service";

const log = createLogger("consumer:retry");

const kafka = new Kafka({
  clientId: "retry-consumer",
  brokers: Config.kafkaBrokers.split(","),
});

const localConfig = validateConfig({
  groupId: process.env.RETRY_CONSUMER_GROUP_ID,
});

const consumer = kafka.consumer({
  groupId: localConfig.groupId,
});

// kafkajs headers are Buffers, so read them as strings thanks haiku 5.5 for function :)
const readHeader = (headers: IHeaders | undefined, key: string) =>
  headers?.[key]?.toString();

const start = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topics: RETRY_TOPIC_NAMES,
    fromBeginning: false,
  });
  await consumer.run({
    eachMessage: async ({ topic, message, pause }) => {
      const target = readHeader(message.headers, "target");
      const originalTopic = readHeader(message.headers, "originalTopic");
      if (!target || !originalTopic) {
        log.error(`missing target or originalTopic on ${topic}`);
        // sendOnCallAlert()
        pause();
        return;
      }

      const targetDetails = KAKFA_CONFIG[target];
      if (!targetDetails) {
        log.error(`no config found for target ${target}`);
        pause();
        return;
      }

      try {
        await sendMessageToTarget(
          { value: message.value?.toString() ?? "" },
          targetDetails.communication_type,
          targetDetails.endpoint,
          targetDetails.timeout,
          target,
          originalTopic,
          readHeader(message.headers, "idempotencyKey") ?? uuidv7(),
        );
      } catch (err) {
        // TODO: forward to the next retry step or the DLQ instead of dropping
        log.error(`retry to ${target} failed`, err);
      }
    },
  });
};

start().catch((err) => {
  log.error("retry consumer failed to start", err);
  process.exitCode = 1;
});
