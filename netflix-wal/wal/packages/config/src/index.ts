import path from "node:path";
import dotenv from "dotenv";
import { z } from "zod";
import { createLogger } from "@wal/logger";
import type { DeliveryTargetCommunicationType } from "./types";

const log = createLogger("schema-validator");

dotenv.config({
  path: path.resolve(__dirname, "../../../.env"),
});

type EnvironmentConfig = Record<string, string | undefined>;

type ValidatedConfig<T extends EnvironmentConfig> = {
  [K in keyof T]: string;
};

export function validateConfig<T extends EnvironmentConfig>(
  config: T,
): ValidatedConfig<T> {
  for (const [key, value] of Object.entries(config)) {
    if (!value) {
      throw new Error(
        `Environment configuration error: "${key}" is not set correctly`,
      );
    }
  }

  return config as ValidatedConfig<T>;
}

const GlobalConfig = validateConfig({
  walDatabaseURI: process.env.DATABASE_URL,
  kafkaBrokers: process.env.KAFKA_BROKERS,
  controlPlaneUrl: process.env.CONTROL_PLANE_URL,
});

export enum TopicOperationType {
  database = "database",
  kafka = "kafka",
}

export interface TopicDetails {
  operationType: TopicOperationType;
  workerWaitTimeInMinutes: number;
  acknowledgement: number;
}

export type TKafkaProducerTopicMapResponse = Record<string, TopicDetails>;

export interface TDeliveryTarget {
  communication_type: DeliveryTargetCommunicationType;
  endpoint: string;
  timeout: number;
  topics: string[];
}

export type TKafkaConsumerTopicMapResponse = Record<string, TDeliveryTarget>;

export default GlobalConfig;

export function validateType<T extends z.ZodType>(
  schema: T,
  object_: unknown,
): z.output<T> {
  try {
    return schema.parse(object_);
  } catch (err: unknown) {
    log.error("error parsing schema");
    throw err;
  }
}
export { z as zod };
