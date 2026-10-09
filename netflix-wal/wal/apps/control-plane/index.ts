import express, { NextFunction, Request, Response } from "express";
import { createLogger } from "@wal/logger";
import { sql } from "drizzle-orm";
import Config from "./config";
import db from "./control-plane-db";
import { reconsiler } from "./reconsiler";
import { getConsumerTopicMap, getTopicMap } from "./topic.service";
import { Admin } from "@platformatic/kafka";
import { validateType } from "@wal/config";
import { createTopicSchema, updateTopicSchema } from "@wal/config/types";
import { createKafkaTopic, updateKafkaTopic } from "./topic.controller";

const log = createLogger("control-plane:api");

const app = express();
app.use(express.json());
log.debug("Kafka brokers configured", Config.kafkaBrokers);

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
  log.info("Reconciliation requested");
  const topicChanged = await reconsiler(admin);
  log.info(`Reconciliation finished, topicChanged=${topicChanged}`);
  res.status(200).send({ topicChanged });
});
app.post("/create-topic", async (req: Request, res: Response) => {
  const topicDetails = validateType(createTopicSchema, req.body);
  const topic = await createKafkaTopic(admin, topicDetails);
  res.status(201).send({ topic });
});

app.patch("/topic/:topicName", async (req: Request, res: Response) => {
  const topicDetails = validateType(updateTopicSchema, req.body);
  const topic = await updateKafkaTopic(
    admin,
    String(req.params.topicName),
    topicDetails,
  );
  if (!topic) {
    res.status(404).send({ error: "Topic not found" });
    return;
  }
  res.status(200).send({ topic });
});

app.get("/get-producer-topic-map", async (_req: Request, res: Response) => {
  const topicMap = await getTopicMap(admin);
  log.debug(`Serving topic map with ${Object.keys(topicMap).length} topics`);
  res.status(200).send({ topicMap: topicMap });
});

app.get("/get-consumer-topic-map", async (_req: Request, res: Response) => {
  await getConsumerTopicMap(admin);
});

app.listen(Config.PORT, () => {
  log.debug("Kafka broker list", Config.kafkaBrokers.split(","));
  log.info(`control-plane for WAL running on ${Config.PORT}`);
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  log.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }
  if (error instanceof Error) {
    log.error(error.message);
  }
  res.status(500).json({ error: "Internal server error" });
});
