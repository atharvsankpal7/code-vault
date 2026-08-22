import express from "express";
import { db } from "@wal/database";
import { wal } from "@wal/database/schema";

const app = express();

app.get("/", (req, res) => {
  res.send("Hello, World!");
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, async () => {
  db.select()
    .from(wal)
    .limit(1)
    .then((_result) => {
      console.log("database connection successful");
    })
    .catch((_error) => {
      console.error("error connection to db");
    });
  console.log(`Server is running on port ${PORT}`);
});
