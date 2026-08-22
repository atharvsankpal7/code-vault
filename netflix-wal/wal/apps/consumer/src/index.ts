import { db, wal } from "./db";

async function checkDatabaseConnection() {
  await db.select().from(wal).limit(1);
  console.log("consumer database connection successful");
}

checkDatabaseConnection().catch((error) => {
  console.error("consumer database connection failed", error);
  process.exitCode = 1;
});
