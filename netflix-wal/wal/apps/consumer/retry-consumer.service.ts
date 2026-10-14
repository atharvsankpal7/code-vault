// tbh this is an app in itself
// we are using kafkajs cause we want pause/resume functionality on the topic which Platformatic currently doesn't provide, check. https://github.com/platformatic/kafka/issues/128. maybe i'll create a PR to solve this issue in the future by talking with some of the folks at platformatic.
import "dotenv/config";
import { IHeaders, Kafka } from "kafkajs";
import { uuidv7 } from "uuidv7";
import { validateConfig } from "@wal/config";
import { createLogger } from "@wal/logger";
import Config, {
  DLQ_TOPIC,
  getRetryStepByTopic,
  RETRY_TOPIC_BY_STEP,
  RETRY_TOPIC_NAMES,
  TRetryStep,
} from "./config";
import { KAKFA_CONFIG, SERVICE_PARTITION_MAP } from "./infra-config";
import {
  buildDLQMessage,
  DeliveryError,
  deliverToTarget,
} from "./consumer.service";

const log = createLogger("consumer:retry");

const kafka = new Kafka({
  clientId: "retry-consumer",
  brokers: Config.kafkaBrokers.split(","),
});

const localConfig = validateConfig({
  groupId: process.env.RETRY_CONSUMER_GROUP_ID,
  maxLinearFailure: process.env.MAX_LINEAR_FAILURE,
});
const linearFailureCountPerTargetMap: Map<string, number> = new Map();
const MAX_LINEAR_FAILURE = parseInt(localConfig.maxLinearFailure);
const consumer = kafka.consumer({
  groupId: localConfig.groupId,
});

const producer = kafka.producer();

// kafkajs headers are Buffers, so read them as strings thanks haiku 5.5 for function :)
const readHeader = (headers: IHeaders | undefined, key: string) =>
  headers?.[key]?.toString();

const headersToStrings = (headers: IHeaders = {}): Record<string, string> =>
  Object.fromEntries(
    Object.entries(headers).flatMap(([key, value]) =>
      value === undefined ? [] : [[key, value.toString()]],
    ),
  );

const start = async () => {
  await producer.connect();
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
        await deliverToTarget(
          message.value?.toString() ?? "",
          targetDetails.communication_type,
          targetDetails.endpoint,
          targetDetails.timeout,
          target,
          originalTopic,
          readHeader(message.headers, "idempotencyKey") ?? uuidv7(),
        );
      } catch (err) {
        if (err instanceof DeliveryError) {
          if (err.kind === "permanent") {
            await producer.send({
              topic: DLQ_TOPIC.name,
              messages: [
                buildDLQMessage({
                  value: message.value?.toString() ?? "",
                  headers: headersToStrings(message.headers),
                  originalTopic,
                  originalOffset: message.offset,
                  error: err,
                }),
              ],
            });
            log.error(`retry to ${target} failed, sent to dlq, err: `, err);
            return;
          }
          if (err.kind === "transient") {
            const currentTargetLinearFailureCount =
              linearFailureCountPerTargetMap.get(target) ??
              linearFailureCountPerTargetMap.set(target, 1).get(target)!;

            if (currentTargetLinearFailureCount >= MAX_LINEAR_FAILURE) {
              // pause and retry after SERVICE_OUTAGE_RETRY_TIME from the config.
              // seek back to the starting point of the message offset
            } else {
              const currentStep = getRetryStepByTopic(topic);
              const nextStep = (currentStep + 1) as TRetryStep;
              const nextRetryTopic = RETRY_TOPIC_BY_STEP[nextStep];
              const partition = SERVICE_PARTITION_MAP[target];

              // no next retry tier, or no partition to route to, so give up and send to the DLQ
              if (!nextRetryTopic || partition === undefined) {
                await producer.send({
                  topic: DLQ_TOPIC.name,
                  messages: [
                    buildDLQMessage({
                      value: message.value?.toString() ?? "",
                      headers: headersToStrings(message.headers),
                      originalTopic,
                      originalOffset: message.offset,
                      error: err,
                    }),
                  ],
                });
                return;
              }

              await producer.send({
                topic: nextRetryTopic.name,
                messages: [
                  {
                    value: message.value?.toString() ?? "",
                    headers: {
                      ...headersToStrings(message.headers),
                      target,
                      originalTopic,
                      originalOffset: message.offset,
                      error: JSON.stringify({ error: err }),
                    },
                    partition,
                  },
                ],
              });
            }
          }
        }
      }
    },
  });
};

start().catch((err) => {
  log.error("retry consumer failed to start", err);
  process.exitCode = 1;
});
