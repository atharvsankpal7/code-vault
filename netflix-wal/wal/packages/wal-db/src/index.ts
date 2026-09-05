import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import Config from "@wal/config";
import * as schema from "./schema";

const pool = new Pool({
  connectionString: Config.walDatabaseURI,
});

const db = drizzle({
  client: pool,
  schema,
});

export { db, pool };
export default db;
