import "dotenv/config";
import { defineConfig } from "drizzle-kit";
import Config from "@wal/config";

export default defineConfig({
  out: "./packages/database/migrations",
  schema: "./packages/database/src/schema.ts",
  dialect: "postgresql",
  dbCredentials: {
    url: Config.databaseUrl,
  },
});
