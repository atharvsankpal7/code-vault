import { startPgBoss } from "./pg-boss";
import { createLogger } from "@wal/logger";

const log = createLogger("producer:worker");
import { refreshTopicMap } from "./kafka-config";
import { sendPendingMessageToKafka } from "./asyncTask.worker";

async function start() {
  await startPgBoss();
  await refreshTopicMap();
  await sendPendingMessageToKafka();

  log.info("WAL producer worker is listening for outbox jobs");
}

start();
