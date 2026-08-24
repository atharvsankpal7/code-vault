import express from "express";
import { createWalDatabase, walOperations } from "@wal/wal-db";

import Config from "./config";

const app = express();
const walDb = createWalDatabase(Config.walDatabaseUrl);

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.listen(Config.PORT, async () => {
  await walDb.select({ id: walOperations.id }).from(walOperations).limit(1);
  console.log("WAL database connection successful");
  console.log(`Server is running on port ${Config.PORT}`);
});
