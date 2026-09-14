import { KAKFA_CONFIG } from ".";
import { boss, WAL_OUTBOX_QUEUE, startPgBoss } from "./pg-boss";
import { kafkaProducer } from "./wal.service";

export const sendPendingMessageToKafka = async () => {
  await startPgBoss();
  await boss.work<{ topic_name: string; message: string }>(
    WAL_OUTBOX_QUEUE,
    { batchSize: 10, perJobResults: true },
    async (messages) => {
      return Promise.all(
        messages.map(async (m) => {
          try {
            const message = m.data;
            const ackValue = KAKFA_CONFIG.get(
              message.topic_name,
            )?.acknowledgement;
            if (ackValue === undefined) {
              throw new Error(
                `topic not found in the kafka config: [messageId] ${m.id}`,
              );
            }
            // perform the computation that we would want to perform and convert the raw message into something that our kafka should strore
            // for demo i would just add something to the message
            message.message = message.message + "hi";
            await kafkaProducer.send({
              messages: [
                {
                  topic: message.topic_name,
                  value: message.message,
                },
              ],
              acks: ackValue,
            });
            console.log(
              `message ${m.id} sent to ${message.topic_name} in kafka`,
            );
            return { id: m.id, status: "completed" };
          } catch (err: any) {
            console.error(`error for ${m.id}: ${err}`);
            return err.fatal
              ? { id: m.id, status: "deadletter", output: err }
              : { id: m.id, status: "failed", output: err };
          }
        }),
      );
    },
  );
};
