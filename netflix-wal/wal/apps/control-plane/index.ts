import express from "express";
import Config from "./config";
import Kafka from "kafkajs";
import { db } from "@wal/database";
import { wal } from "@wal/database/schema";

const app = express();

app.get("/health", async (_req, res) => {
  const start = performance.now();
  await db.select({}).from(wal).limit(1);
  const end = performance.now();

  res.send(`control-plane is healthy, Db-ping=[${end - start}ms]`);
});

app.listen(Config.PORT, () => {
  console.log(`control-plane for WAL running on ${Config.PORT}`);
});
