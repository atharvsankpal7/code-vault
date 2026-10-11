import { Consumer, stringDeserializers } from "@platformatic/kafka";
import Config from "./config";
import { KAKFA_CONFIG } from "./kafka-config";
import { createLogger } from "@wal/logger";
import {
  getIdempotencyKey,
  isRetryableError,
  sendMessageToTarget,
} from "./consumer.service";
import {
  FailedDelivery,
  produceRetry,
  produceToDLQ,
} from "./retry/retry-producer";

const log = createLogger(`consumer:${Config.serviceSpecificClientName}`);

export const consumer = new Consumer({
  clientId: Config.serviceSpecificClientId,
  groupId: Config.serviceSpecificClientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
  deserializers: stringDeserializers,
});
export async function startConsumer() {
  const topicDetails = KAKFA_CONFIG[Config.serviceSpecificClientName];
  if (!topicDetails) {
    throw new Error(
      `config for topic not found, topic_name: [${Config.serviceSpecificClientName}]`,
    );
  }
  const stream = await consumer.consume({
    topics: topicDetails.topics,
    sessionTimeout: 10000,
    heartbeatInterval: 500,
  });

  stream.on("data", async (message) => {
    log.info(
      `[${message.topic}:${message.partition}@${message.offset}]`,
      message.key,
      message.value,
    );
    try {
      try {
        // todo: check how can we batch the message sending
        await sendMessageToTarget(
          message,
          topicDetails.communication_type,
          topicDetails.endpoint,
          topicDetails.timeout,
          Config.serviceSpecificClientName,
          message.topic,
        );
      } catch (err) {
        log.error("Processing failed", err);
        const failed: FailedDelivery = {
          value: message.value,
          targetName: Config.serviceSpecificClientName,
          idempotencyKey: getIdempotencyKey(
            message,
            Config.serviceSpecificClientName,
            message.topic,
          ),
          originalTopic: message.topic,
          originalPartition: message.partition,
          originalOffset: message.offset.toString(),
          firstFailedAt: Date.now(),
          retryAttempt: 0,
          error: err,
        };
        if (isRetryableError(err)) {
          await produceRetry(failed);
        } else {
          await produceToDLQ(failed, "non retryable error");
        }
      }
      // commit for both success and failure as failure mode of our function puts message in another durable state
      await message.commit();
    } catch (err) {
      // the failure could not be made durable, leave the offset uncommitted so
      // the message is read again instead of being lost
      log.error("Failed to hand off failed message, not committing", err);
    }
  });

  stream.on("error", (err) => console.error("Stream error", err));

  return stream;
}
