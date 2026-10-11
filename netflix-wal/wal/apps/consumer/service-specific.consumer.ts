import { Consumer, stringDeserializers } from "@platformatic/kafka";
import Config from "./config";
import { KAKFA_CONFIG } from "./infra-config";
import { createLogger } from "@wal/logger";
import {
  DeliveryError,
  sendMessageToTarget,
  sendToDLQ,
  sendToRetry,
} from "./consumer.service";
import { uuidv7 } from "uuidv7";

const log = createLogger(`consumer:${Config.serviceSpecificClientName}`);

export const consumer = new Consumer({
  clientId: Config.serviceSpecificClientId,
  groupId: Config.serviceSpecificClientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
  deserializers: stringDeserializers,
});
export async function startConsumer() {
  const targetDetails = KAKFA_CONFIG[Config.serviceSpecificClientName];
  if (!targetDetails) {
    throw new Error(
      `config for topic not found, topic_name: [${Config.serviceSpecificClientName}]`,
    );
  }
  const stream = await consumer.consume({
    topics: targetDetails.topics,
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
      await sendMessageToTarget(
        message,
        targetDetails.communication_type,
        targetDetails.endpoint,
        targetDetails.timeout,
        Config.serviceSpecificClientName,
        message.topic,
        message.headers.get("idempotencyKey") ||
          message.headers
            .set("idempotencyKey", uuidv7())
            .get("idempotencyKey")!,
      );
    } catch (err) {
      const deliveryError =
        err instanceof DeliveryError
          ? err
          : new DeliveryError(
              String(err),
              "transient",
              Config.serviceSpecificClientName,
              err,
            );
      try {
        if (deliveryError.kind === "permanent") {
          await sendToDLQ(message, deliveryError);
          message.commit();
        } else {
          await sendToRetry(1, message, deliveryError);
          message.commit();
        }
      } catch (err) {
        // sendOnCallAlert()
      }
    }
  });

  stream.on("error", (err) => console.error("Stream error", err));

  return stream;
}
