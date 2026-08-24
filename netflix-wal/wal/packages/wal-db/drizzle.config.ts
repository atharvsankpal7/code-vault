import GlobalConfig from "@wal/config";
import "dotenv/config";
import { defineConfig } from "drizzle-kit";

const databaseUrl = GlobalConfig.;

if (!databaseUrl) {
  throw new Error(
    'Environment configuration error: "WAL_DATABASE_URL" is not set correctly',
  );
}

export default defineConfig({
  schema: "./packages/wal-db/src/schema.ts",
  out: "./packages/wal-db/migrations",
  dialect: "postgresql",
  dbCredentials: {
    url: databaseUrl,
  },
  strict: true,
  verbose: true,
});
