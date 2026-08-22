import { drizzle } from "drizzle-orm/node-postgres";
import Config from "@wal/config";

export const db = drizzle(Config.databaseUrl);
