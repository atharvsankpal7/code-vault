import GlobalConfig from "@wal/config";
import { defineConfig } from "drizzle-kit";

const databaseUrl = GlobalConfig.walDatabaseURI;

if (!databaseUrl) {
  throw new Error(
    'Environment configuration error: "WAL_DATABASE_URL" is not set correctly',
  );
}

export default defineConfig({
  schema: "./src/schema.ts",
  out: "./migrations",
  dialect: "postgresql",
  dbCredentials: {
    url: databaseUrl,
  },
  strict: true,
  verbose: true,
});
