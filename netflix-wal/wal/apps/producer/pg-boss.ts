import { PgBoss } from "pg-boss";
import Config from "./config";

export const WAL_OUTBOX_QUEUE = "wal-outbox";
export const WAL_OUTBOX_DLQ = "wal-outbox.dlq";

export const boss = new PgBoss({
  connectionString: Config.walDatabaseURI,
  useListenNotify: true,
  reindex: { maxIndexBytes: 1024 * 1024 * 1024 }, //reindex every 1GB
});

boss.on("error", console.error);

export async function startPgBoss() {
  await boss.start();
  await boss.createQueue(WAL_OUTBOX_DLQ);
  await boss.createQueue(WAL_OUTBOX_QUEUE, {
    retryLimit: 3,
    retryDelay: 5,
    retryBackoff: true,
    expireInSeconds: 15 * 60, // default; override per job from topic config
    deadLetter: WAL_OUTBOX_DLQ,
    notify: true,
  });
}
