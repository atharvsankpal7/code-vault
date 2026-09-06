import express, { NextFunction, Request, Response } from "express";
import { sql } from "drizzle-orm";
import Config from "./config";
import db from "./control-plane-db";
import { reconsiler } from "./reconsiler";
import { getTopicMap } from "./topic.service";
import { Admin } from "@platformatic/kafka";

const app = express();

// const runReconsiler = () => {
//   setTimeout(async () => {
//     reconsiler().then(() => {
//       runReconsiler();
//     });
//   }, 15000);
// };

app.get("/health", async (_req: Request, res: Response) => {
  const start = performance.now();
  await db.execute(sql`select 1`);
  const end = performance.now();
  res.status(200).send(`control-plane is healthy, Db-ping=[${end - start}ms]`);
});
const admin = new Admin({
  clientId: Config.kafkaClientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
});
app.get("/reconsile", async (_req: Request, res: Response) => {
  const topicChanged = await reconsiler(admin);
  res.status(200).send({ topicChanged });
});

app.get("/get-topic-map", async (_req: Request, res: Response) => {
  const topicMap = await getTopicMap();
  res.status(200).send(topicMap);
});

// Must be registered after all routes so it handles errors from the application.
app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  console.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, () => {
  console.log(`control-plane for WAL running on ${Config.PORT}`);
});

// runReconsiler();
