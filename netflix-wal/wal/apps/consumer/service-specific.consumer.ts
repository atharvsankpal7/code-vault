import { Consumer, stringDeserializers } from "@platformatic/kafka";
import Config from "./config";
import { KAKFA_CONFIG } from "./kafka-config";
import { createLogger } from "@wal/logger";
import { sendMessageToTarget } from "./consumer.service";

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
      // if retry is exhauseted send to dlq else put it in the sqs for retry.
    } finally {
      // commit for both success and failure as failure mode of our function puts message in another durable state
      await message.commit();
    }
  });

  stream.on("error", (err) => console.error("Stream error", err));

  return stream;
}
