import { Consumer, stringDeserializers } from "@platformatic/kafka";
import Config from "./config";

export const consumer = new Consumer({
  clientId: Config.serviceSpecificClientId,
  groupId: Config.serviceSpecificClientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
  deserializers: stringDeserializers,
});

export async function startConsumer() {
  const stream = await consumer.consume({
    topics: ["orders"],
    autocommit: false, // commit manually after successful processing
    sessionTimeout: 10000,
    heartbeatInterval: 500,
  });

  stream.on("data", async (message) => {
    try {
      console.log(
        `[${message.topic}:${message.partition}@${message.offset}]`,
        message.key,
        message.value,
      );
      // ... your business logic ...
      await message.commit();
    } catch (err) {
      console.error("Processing failed", err);
      // don't commit, so it can be retried / sent to a DLQ
    }
  });

  stream.on("error", (err) => console.error("Stream error", err));

  return stream;
}
