import express, { NextFunction, Request, Response } from "express";
import { createLogger } from "@wal/logger";
import Config from "./config";
import { generateWal } from "./wal.service";
import { startPgBoss } from "./pg-boss";
import { refreshTopicMap } from "./kafka-config";

const log = createLogger("producer:api");

const app = express();
app.use(express.json());

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});
app.get("/refresh-kafka-map", async (_req, res) => {
  await refreshTopicMap();
  res.send("Kafka map refreshed");
});

app.post("/generate-wal", async (req, res) => {
  const result = await generateWal(req.body);
  if (!result) {
    return res.status(500).send("Failed to generate WAL");
  }
  res.status(200).send("WAL generated");
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  log.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, async () => {
  await refreshTopicMap();
  await startPgBoss();
  log.info("WAL database connection successful");
  log.info(`Server is running on port ${Config.PORT}`);
});
