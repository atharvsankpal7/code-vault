import "dotenv/config";
import GlobalConfig, { validateConfig } from "@wal/config";
import { uuidv7 } from "uuidv7";

const consumerServices = validateConfig({
  SPECIFIC_CONSUMER_SERVICE: process.env.SPECIFIC_CONSUMER_SERVICE,
  SHARED_CONSUMER_SERVICES: process.env.SHARED_CONSUMER_SERVICES,
});

const serviceSpecificGroupId = `service-${consumerServices.SPECIFIC_CONSUMER_SERVICE}-consumer-`;
const sharedConsumerGroupId = `service-${consumerServices.SHARED_CONSUMER_SERVICES.split(
  ",",
)
  .map((service) => service)
  .join("-")}-consumer`;
const localConfig = validateConfig({
  PORT: process.env.PORT,
  serviceSpecificGroupId,
  sharedConsumerGroupId,
  serviceSpecificClientId: serviceSpecificGroupId + uuidv7(),
  sharedConsumerClientId: sharedConsumerGroupId + uuidv7(),
  serviceSpecificClientName: consumerServices.SPECIFIC_CONSUMER_SERVICE,
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export type TRetryStep = 1 | 2 | 3;

export type TKafkaTopicSpec = {
  name: string;
  partitions: number;
  retentionMs: string;
};

const FOURTEEN_DAYS_MS = String(14 * 24 * 60 * 60 * 1000);
const RETAIN_FOREVER = "-1";

export const RETRY_TOPIC_BY_STEP: Record<TRetryStep, TKafkaTopicSpec> = {
  1: { name: "retry-10s", partitions: 64, retentionMs: FOURTEEN_DAYS_MS },
  2: { name: "retry-1m", partitions: 64, retentionMs: FOURTEEN_DAYS_MS },
  3: { name: "retry-5m", partitions: 64, retentionMs: FOURTEEN_DAYS_MS },
};
export const RETRY_TOPIC_NAMES = Object.values(RETRY_TOPIC_BY_STEP).map(
  (e) => e.name,
);
export const DLQ_TOPIC: TKafkaTopicSpec = {
  name: "dlq",
  partitions: 12,
  retentionMs: RETAIN_FOREVER,
};

export default Config;
