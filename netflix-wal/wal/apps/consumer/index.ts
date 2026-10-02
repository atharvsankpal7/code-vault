import express, { NextFunction, Request, Response } from "express";
import { createLogger } from "@wal/logger";
import Config from "./config";

const log = createLogger("consumer:api");

const app = express();
app.use(express.json());

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  log.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, async () => {
  log.info("WAL database connection successful");
  log.info(`Server is running on port ${Config.PORT}`);
});
