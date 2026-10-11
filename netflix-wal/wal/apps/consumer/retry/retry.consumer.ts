import GlobalConfig, { validateConfig } from "@wal/config";
import { createLogger } from "@wal/logger";
import { Kafka, KafkaMessage } from "kafkajs";
import { deliverToTarget, isRetryableError } from "../consumer.service";
import { KAKFA_CONFIG } from "../kafka-config";
import {
  BreakerState,
  forgetBreaker,
  getBreakerState,
  getPausedUntil,
  halfOpen,
  recordFailure,
  recordSuccess,
} from "./circuit-breaker";
import {
  RETRY_TIERS,
  RetryHeaders,
  WATCHDOG_INTERVAL_MS,
} from "./retry-config";
import { FailedDelivery, produceRetry, produceToDLQ } from "./retry-producer";

const log = createLogger("consumer:retry");

// which tiers this instance consumes, e.g. RETRY_TOPICS=wal-retry-10s,wal-retry-1m
const retryEnv = validateConfig({
  RETRY_TOPICS: process.env.RETRY_TOPICS,
});

const retryTopics = retryEnv.RETRY_TOPICS.split(",").map((t) => t.trim());
for (const topic of retryTopics) {
  if (!RETRY_TIERS.some((tier) => tier.topic === topic)) {
    throw new Error(`RETRY_TOPICS contains unknown retry tier [${topic}]`);
  }
}

// instances consuming different tier sets must not share a group
const retryGroupId = `retry-consumer-${retryTopics.join("-")}`;

const kafka = new Kafka({
  clientId: retryGroupId,
  brokers: GlobalConfig.kafkaBrokers.split(","),
});

export const retryConsumer = kafka.consumer({ groupId: retryGroupId });

const partitionKey = (topic: string, partition: number) =>
  `${topic}:${partition}`;

const resumeTimers = new Map<string, NodeJS.Timeout>();

const header = (message: KafkaMessage, name: string) =>
  message.headers?.[name]?.toString();

const toFailedDelivery = (
  message: KafkaMessage,
  error: unknown,
): FailedDelivery => ({
  value: message.value?.toString() ?? "",
  targetName: header(message, RetryHeaders.targetName) ?? "",
  idempotencyKey: header(message, RetryHeaders.idempotencyKey) ?? "",
  originalTopic: header(message, RetryHeaders.originalTopic) ?? "",
  originalPartition: Number(header(message, RetryHeaders.originalPartition)),
  originalOffset: header(message, RetryHeaders.originalOffset) ?? "",
  firstFailedAt: Number(header(message, RetryHeaders.firstFailedAt)),
  retryAttempt: Number(header(message, RetryHeaders.retryAttempt) ?? 1),
  error,
});

// pause stops fetching only on this instance, seek makes the fetch after the
// resume start again at `offset` instead of after the batch we abandoned
const pausePartition = (
  topic: string,
  partition: number,
  offset: string,
  resumeAt: number,
) => {
  const key = partitionKey(topic, partition);
  retryConsumer.pause([{ topic, partitions: [partition] }]);
  retryConsumer.seek({ topic, partition, offset });

  clearTimeout(resumeTimers.get(key));
  resumeTimers.set(
    key,
    setTimeout(
      () => resumePartition(topic, partition),
      Math.max(resumeAt - Date.now(), 0),
    ),
  );
};

const resumePartition = (topic: string, partition: number) => {
  const key = partitionKey(topic, partition);
  resumeTimers.delete(key);
  // a breaker that paused this partition lets exactly one probe through next
  halfOpen(key);
  try {
    retryConsumer.resume([{ topic, partitions: [partition] }]);
  } catch (err) {
    // the watchdog resumes partitions that are paused without a timer
    log.error(`Failed to resume ${key}`, err);
  }
};

const handleMessage = async (
  topic: string,
  partition: number,
  message: KafkaMessage,
): Promise<"done" | "paused"> => {
  const key = partitionKey(topic, partition);
  const dueAt = Number(header(message, RetryHeaders.dueAt) ?? 0);

  if (dueAt > Date.now()) {
    pausePartition(topic, partition, message.offset, dueAt);
    return "paused";
  }

  const targetName = header(message, RetryHeaders.targetName) ?? "";
  const target = KAKFA_CONFIG[targetName];
  if (!target) {
    await produceToDLQ(
      toFailedDelivery(message, new Error("unknown target")),
      `target [${targetName}] not found in consumer topic map`,
    );
    return "done";
  }

  try {
    await deliverToTarget(
      message.value?.toString() ?? "",
      header(message, RetryHeaders.idempotencyKey) ?? "",
      target.communication_type,
      target.endpoint,
      target.timeout,
      header(message, RetryHeaders.originalTopic) ?? "",
    );
    if (getBreakerState(key) !== BreakerState.closed) {
      log.info(`Target ${targetName} recovered, closing breaker for ${key}`);
    }
    recordSuccess(key);
    return "done";
  } catch (err) {
    const failed = toFailedDelivery(message, err);

    if (!isRetryableError(err)) {
      await produceToDLQ(failed, "non retryable error");
      return "done";
    }

    if (recordFailure(key)) {
      // target looks down: hold this message without spending an attempt
      log.warn(
        `Breaker open for ${targetName}, pausing ${key} until ${new Date(getPausedUntil(key)).toISOString()}`,
      );
      pausePartition(topic, partition, message.offset, getPausedUntil(key));
      return "paused";
    }

    await produceRetry(failed);
    return "done";
  }
};

const startWatchdog = () =>
  setInterval(() => {
    for (const { topic, partitions } of retryConsumer.paused()) {
      for (const partition of partitions) {
        const key = partitionKey(topic, partition);
        if (!resumeTimers.has(key)) {
          log.warn(`${key} paused without a resume timer, resuming`);
          resumePartition(topic, partition);
        }
      }
    }
  }, WATCHDOG_INTERVAL_MS);

export async function startRetryConsumer() {
  // a rebalance hands partitions to other members, drop state for the ones we lost
  retryConsumer.on(retryConsumer.events.GROUP_JOIN, ({ payload }) => {
    const assigned = new Set(
      Object.entries(payload.memberAssignment).flatMap(([topic, partitions]) =>
        partitions.map((partition) => partitionKey(topic, partition)),
      ),
    );
    for (const [key, timer] of resumeTimers) {
      if (!assigned.has(key)) {
        clearTimeout(timer);
        resumeTimers.delete(key);
        forgetBreaker(key);
      }
    }
    log.info(`Retry consumer assigned ${assigned.size} partitions`);
  });

  await retryConsumer.connect();
  await retryConsumer.subscribe({ topics: retryTopics, fromBeginning: true });

  await retryConsumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    // partitions run in parallel, messages inside one partition stay sequential
    partitionsConsumedConcurrently: 8,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          break;
        }

        const result = await handleMessage(
          batch.topic,
          batch.partition,
          message,
        );
        if (result === "paused") {
          break;
        }

        // commit only after the message was delivered or its forward was acked.
        // commitOffsetsIfNecessary is a no-op with autoCommit off, so commit
        // explicitly; kafka expects the offset of the next message to read
        resolveOffset(message.offset);
        await retryConsumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        await heartbeat();
      }
    },
  });

  startWatchdog();
  log.info(`Retry consumer started for ${retryTopics.join(", ")}`);
}
