import express from "express";
import Config from "./config";

const app = express();

app.get("/hi", (_req, res) => {
  res.send("Hello, World!");
});

app.listen(Config.PORT, async () => {
  console.log(`Server is running on port ${Config.PORT}`);
});
