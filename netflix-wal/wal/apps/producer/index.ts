import express from "express";

import Config from "./config";

const app = express();

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.get("/generate-wal", (_req, res) => {
  await generateWal(req.body);
});
app.listen(Config.PORT, async () => {
  await walDb.select({ id: walOperations.id }).from(walOperations).limit(1);
  console.log("WAL database connection successful");
  console.log(`Server is running on port ${Config.PORT}`);
});
