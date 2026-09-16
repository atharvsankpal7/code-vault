import express, { NextFunction, Request, Response } from "express";

import Config from "./config";
import { generateWal } from "./wal.service";
import { startPgBoss } from "./pg-boss";
import { refreshTopicMap } from "./kafka-config";

const app = express();
app.use(express.json());
await startPgBoss();

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
  console.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, async () => {
  await refreshTopicMap();
  console.log("WAL database connection successful");
  console.log(`Server is running on port ${Config.PORT}`);
});
