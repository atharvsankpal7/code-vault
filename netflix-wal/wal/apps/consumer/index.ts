import express, { NextFunction, Request, Response } from "express";
import { createLogger } from "@wal/logger";
import Config from "./config";
import { refreshConsumerTopicMap } from "./kafka-config";

const log = createLogger("consumer:api");

const app = express();
app.use(express.json());

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.get("/refresh-consumer-kafka-map", async (_req: Request, res: Response) => {
  await refreshConsumerTopicMap();
  res.send("fetched updated kafka topic-service relation");
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  log.error("Unhandled request error:", error);

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, async () => {
  await refreshConsumerTopicMap();
  log.info(`Consumer is running on port ${Config.PORT}`);
});
