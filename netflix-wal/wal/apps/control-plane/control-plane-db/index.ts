import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import Config from "../config";
import * as schema from "./schema";
import { createLogger } from "@wal/logger";

const log = createLogger("control-plane:db");

const pool = new Pool({
  connectionString: Config.controlPlaneDatabaseUrl,
});

pool.on("error", (error) => log.error("Postgres pool error", error));

const db = drizzle({
  client: pool,
  schema,
});

export { db, pool };
export default db;
