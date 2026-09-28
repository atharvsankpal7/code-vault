import express, { NextFunction, Request, Response } from "express";

import Config from "./config";

const app = express();
app.use(express.json());

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  console.error("Unhandled request error:", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).json({ error: "Internal server error" });
});

app.listen(Config.PORT, async () => {
  console.log("WAL database connection successful");
  console.log(`Server is running on port ${Config.PORT}`);
});
