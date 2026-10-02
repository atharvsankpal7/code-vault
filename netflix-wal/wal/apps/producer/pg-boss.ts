import { PgBoss } from "pg-boss";
import { createLogger } from "@wal/logger";
import Config from "./config";

const log = createLogger("producer:pg-boss");

export const WAL_OUTBOX_QUEUE = "wal-outbox";
export const WAL_OUTBOX_DLQ = "wal-outbox.dlq";

export const boss = new PgBoss({
  connectionString: Config.walDatabaseURI,
  useListenNotify: true,
  reindex: { maxIndexBytes: 1024 * 1024 * 1024 }, //reindex every 1GB
});

boss.on("error", (error) => log.error("pg-boss error", error));

export async function startPgBoss() {
  log.debug("Starting pg-boss");
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
  log.info(`pg-boss started, queues ready: ${WAL_OUTBOX_QUEUE}, ${WAL_OUTBOX_DLQ}`);
}
