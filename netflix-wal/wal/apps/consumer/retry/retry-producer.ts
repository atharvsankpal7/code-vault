import GlobalConfig from "@wal/config";
import { createLogger } from "@wal/logger";
import { Kafka, Producer } from "kafkajs";
import {
  DLQ_TOPIC,
  JITTER_RATIO,
  RETRY_TIERS,
  RetryHeaders,
} from "./retry-config";
import { SERVICE_PARTITION_MAP } from "./service-partition-map";

const log = createLogger("consumer:retry-producer");

export interface FailedDelivery {
  value: string;
  targetName: string;
  idempotencyKey: string;
  originalTopic: string;
  originalPartition: number;
  originalOffset: string;
  firstFailedAt: number;
  // attempts already made, the main consumer's failed send counts as 0
  retryAttempt: number;
  error: unknown;
}

const kafka = new Kafka({
  clientId: "wal-retry-producer",
  brokers: GlobalConfig.kafkaBrokers.split(","),
});

// the promise is cached, not the producer, so concurrent first sends share one
// connect instead of each opening their own producer
let producer: Promise<Producer> | undefined;

const getProducer = (): Promise<Producer> => {
  if (!producer) {
    const instance = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });
    producer = instance.connect().then(
      () => instance,
      (err) => {
        producer = undefined;
        throw err;
      },
    );
  }
  return producer;
};

const withJitter = (delayMs: number) =>
  Math.round(delayMs * (1 + (Math.random() * 2 - 1) * JITTER_RATIO));

const errorMessage = (error: unknown) =>
  (error instanceof Error ? error.message : String(error)).slice(0, 1000);

const buildHeaders = (failed: FailedDelivery, dueAt: number) => ({
  [RetryHeaders.targetName]: failed.targetName,
  [RetryHeaders.retryAttempt]: String(failed.retryAttempt + 1),
  [RetryHeaders.dueAt]: String(dueAt),
  [RetryHeaders.idempotencyKey]: failed.idempotencyKey,
  [RetryHeaders.originalTopic]: failed.originalTopic,
  [RetryHeaders.originalPartition]: String(failed.originalPartition),
  [RetryHeaders.originalOffset]: failed.originalOffset,
  [RetryHeaders.firstFailedAt]: String(failed.firstFailedAt),
  [RetryHeaders.lastError]: errorMessage(failed.error),
});

export const produceToDLQ = async (failed: FailedDelivery, reason: string) => {
  log.warn(
    `Sending ${failed.idempotencyKey} for ${failed.targetName} to ${DLQ_TOPIC}: ${reason}`,
  );
  await (
    await getProducer()
  ).send({
    topic: DLQ_TOPIC,
    acks: -1,
    messages: [
      {
        // keyed by target so a replay tool can drain one target at a time
        key: failed.targetName,
        value: failed.value,
        headers: buildHeaders(failed, Date.now()),
      },
    ],
  });
};

// sends the message to the tier after `retryAttempt`, or to the dlq once every
// tier has been tried. resolves only after kafka acknowledged the write, so the
// caller can commit the source offset safely.
export const produceRetry = async (failed: FailedDelivery) => {
  const tier = RETRY_TIERS[failed.retryAttempt];
  if (!tier) {
    return produceToDLQ(failed, "retry attempts exhausted");
  }

  const partition = SERVICE_PARTITION_MAP[failed.targetName];
  if (partition === undefined) {
    return produceToDLQ(failed, "target has no retry partition assigned");
  }

  const dueAt = Date.now() + withJitter(tier.delayMs);
  log.info(
    `Retrying ${failed.idempotencyKey} for ${failed.targetName} via ${tier.topic}:${partition}, attempt ${failed.retryAttempt + 1}`,
  );
  await (
    await getProducer()
  ).send({
    topic: tier.topic,
    acks: -1,
    messages: [
      {
        partition,
        key: failed.targetName,
        value: failed.value,
        headers: buildHeaders(failed, dueAt),
      },
    ],
  });
};

export const disconnectRetryProducer = async () => {
  const instance = await producer;
  producer = undefined;
  await instance?.disconnect();
};
