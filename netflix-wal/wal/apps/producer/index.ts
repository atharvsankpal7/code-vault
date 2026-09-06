import express from "express";

import Config from "./config";
import db from "@wal/wal-db";
import { wal_outbox } from "@wal/wal-db/schema";
import { KafkaConsumerGroupStates } from "@platformatic/kafka";
import { generateWal } from "./wal.service";

enum TopicOperationType {
  database = "database",
  kafka = "kafka",
}
interface topicDetails {
  operationType: TopicOperationType;
  leaseWaitTimeInMinutes: number;
  acknowledgement: number;
}

export let KAKFA_CONFIG: Map<string, topicDetails> = new Map();

const app = express();

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.get("/generate-wal", async (req, res) => {
  await generateWal(req.body);
});
app.listen(Config.PORT, async () => {
  await db.select({ id: wal_outbox.id }).from(wal_outbox).limit(1);
  const response = await fetch(`${Config.controlPlaneUrl}`);
  const data = await response.json();
  KAKFA_CONFIG = new Map(data);
  console.log("WAL database connection successful");
  console.log(`Server is running on port ${Config.PORT}`);
});
