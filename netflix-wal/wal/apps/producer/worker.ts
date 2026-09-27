import { startPgBoss } from "./pg-boss";
import { refreshTopicMap } from "./kafka-config";
import { sendPendingMessageToKafka } from "./asyncTask.worker";

await startPgBoss();
await refreshTopicMap();
await sendPendingMessageToKafka();

console.log("WAL producer worker is listening for outbox jobs");
