import express, { NextFunction, Request, Response } from "express";

import Config from "./config";
import db from "@wal/wal-db";
import { wal_outbox } from "@wal/wal-db/schema";
import { generateWal } from "./wal.service";
import { TKafkaTopicMapResponse } from "@wal/config";

export let KAKFA_CONFIG: TKafkaTopicMapResponse = new Map();

const app = express();
const getTopicMap = async (): Promise<TKafkaTopicMapResponse> => {
  const response = await fetch(`${Config.controlPlaneUrl}/get-topic-map`);
  const data: { topicMap: TKafkaTopicMapResponse } = await response.json();
  return data.topicMap;
};

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.get("/refresh-kafka-map", async (req, res) => {
  KAKFA_CONFIG = await getTopicMap();
});

app.get("/generate-wal", async (req, res) => {
  await generateWal(req.body);
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  console.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, async () => {
  await db.select({ id: wal_outbox.id }).from(wal_outbox).limit(1);
  KAKFA_CONFIG = await getTopicMap();
  console.log("WAL database connection successful");
  console.log(`Server is running on port ${Config.PORT}`);
});
