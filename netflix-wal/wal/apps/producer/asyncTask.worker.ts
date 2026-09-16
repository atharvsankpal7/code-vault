import { boss, WAL_OUTBOX_QUEUE } from "./pg-boss";
import { kafkaProducer } from "./wal.service";
import { performCpuTask } from "./cpu.worker.service";
import { KAKFA_CONFIG } from "./kafka-config";
export const sendPendingMessageToKafka = async () => {
  await boss.work<{ topic_name: string; message: string }>(
    WAL_OUTBOX_QUEUE,
    {
      batchSize: 1,
      localConcurrency: 4,
      perJobResults: true,
    },
    async (messages) => {
      return Promise.all(
        messages.map(async (m) => {
          try {
            const message = m.data;

            const ackValue = KAKFA_CONFIG[message.topic_name]?.acknowledgement;

            if (ackValue === undefined) {
              throw new Error(
                `topic not found in the kafka config: [messageId] ${m.id}`,
              );
            }

            const transformedMessage = await performCpuTask(
              message.message,
              500_000,
            );

            await kafkaProducer.send({
              messages: [
                {
                  topic: message.topic_name,
                  value: transformedMessage,
                },
              ],
              acks: ackValue,
            });

            console.log(
              `message ${m.id} sent to ${message.topic_name} in kafka`,
            );

            return {
              id: m.id,
              status: "completed",
            };
          } catch (err: any) {
            console.error(`error for ${m.id}: ${err}`);

            return err.fatal
              ? {
                  id: m.id,
                  status: "deadletter",
                  output: err,
                }
              : {
                  id: m.id,
                  status: "failed",
                  output: err,
                };
          }
        }),
      );
    },
  );
};
