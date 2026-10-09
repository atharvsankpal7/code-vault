import { Consumer, stringDeserializers } from "@platformatic/kafka";
import Config from "./config";
import { KAKFA_CONFIG } from "./kafka-config";
import { createLogger } from "@wal/logger";

const log = createLogger(`consumer:${Config.serviceSpecificClientTopicName}`);

export const consumer = new Consumer({
  clientId: Config.serviceSpecificClientId,
  groupId: Config.serviceSpecificClientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
  deserializers: stringDeserializers,
});
export async function startConsumer() {
  if (!KAKFA_CONFIG[Config.serviceSpecificClientTopicName]) {
    throw new Error(
      `config for topic not found, topic_name: [${Config.serviceSpecificClientTopicName}]`,
    );
  }
  const stream = await consumer.consume({
    topics: [Config.serviceSpecificClientTopicName],
    sessionTimeout: 10000,
    heartbeatInterval: 500,
  });

  stream.on("data", async (message) => {
    try {
      log.info(
        `[${message.topic}:${message.partition}@${message.offset}]`,
        message.key,
        message.value,
      );
      // send the message to the dedicated service using the given communicationn type
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
