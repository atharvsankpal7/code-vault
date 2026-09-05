import express, { NextFunction, Request, Response } from "express";
import { sql } from "drizzle-orm";
import Config from "./config";
import db from "./control-plane-db";
import { reconsiler } from "./reconsiler";

const app = express();

// const runReconsiler = () => {
//   setTimeout(async () => {
//     reconsiler().then(() => {
//       runReconsiler();
//     });
//   }, 15000);
// };

app.get("/health", async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const start = performance.now();
    await db.execute(sql`select 1`);
    const end = performance.now();
    res
      .status(200)
      .send(`control-plane is healthy, Db-ping=[${end - start}ms]`);
  } catch (error) {
    next(error);
  }
});

app.get(
  "/reconsile",
  async (_req: Request, res: Response, next: NextFunction) => {
    try {
      const topicChanged = await reconsiler();
      res.status(200).send({ topicChanged });
    } catch (error) {
      next(error);
    }
  },
);

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
