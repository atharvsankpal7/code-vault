import db from "@wal/wal-db";
import { desc, eq, inArray } from "drizzle-orm";
import { uuidv7 } from "uuidv7";
import { Producer, stringSerializers } from "@platformatic/kafka";
import Config from "./config";
import { wal_outbox } from "@wal/wal-db/schema";
import { KAKFA_CONFIG } from ".";
import { response } from "express";

const WORKER_LEASE_MS = 60 * 1000;
const workerId = uuidv7();

export const sendMessageToKafka = async () => {
  const producer = new Producer({
    clientId: Config.clientId,
    bootstrapBrokers: Config.kafkaBrokers.split(","),
    serializers: stringSerializers,
  });
  const now = new Date();
  const messageFromDb = await db
    .update(wal_outbox)
    .set({
      workerId,
      status: "processing",
      processStartedAt: now,
      processingTimeOut: new Date(now.getTime() + WORKER_LEASE_MS),
    })
    .where(
      eq(
        wal_outbox.id,
        db
          .select()
          .from(wal_outbox)
          .where(eq(wal_outbox.status, "pending"))
          .limit(10)
          .orderBy(wal_outbox.created_at)
          .for("update", { skipLocked: true }),
      ),
    )
    .returning({
      id: wal_outbox.id,
      topic_name: wal_outbox.topic_name,
      message: wal_outbox.message,
    });
  if (messageFromDb.length === 0) {
    return;
  }
  console.log(
    `started processing for messages with ids ${messageFromDb.map((m) => m.id)}`,
  );

  // perform the computation that we would want to perform and convert the raw message into something that our kafka should strore
  // for demo i would just add something to the message
  for (const message of messageFromDb) {
    const ackValue = KAKFA_CONFIG.get(message.topic_name)?.acknowledgement;
    if (ackValue === undefined) {
      await db
        .update(wal_outbox)
        .set({ status: "failed", error: "topic not found in the kafka config" })
        .where(eq(wal_outbox.id, message.id));
      console.log(
        `error for ${message.id}: topic not found in the kafka config`,
      );
      continue;
    }

    message.message = message.message + "hi";
    try {
      await producer.send({
        messages: [
          {
            topic: message.topic_name,
            value: message.message,
          },
        ],
        acks: ackValue,
      });
      console.log(
        `message ${message.id} sent to ${message.topic_name} in kafka`,
      );
    } catch (error) {
      console.error(`error for ${message.id}: ${error}`);
      await db
        .update(wal_outbox)
        .set({
          status: "failed",
          error: error instanceof Error ? error.message : String(error),
        })
        .where(eq(wal_outbox.id, message.id));
      continue;
    }

    await db
      .update(wal_outbox)
      .set({ status: "done", processCompletedAt: new Date() })
      .where(eq(wal_outbox.id, message.id));
    console.log(`Successfully sent message to ${message.topic_name}`);
  }
};
