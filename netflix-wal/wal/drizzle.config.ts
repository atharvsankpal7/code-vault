import "dotenv/config";
import { defineConfig } from "drizzle-kit";
import Config from "./config";

export default defineConfig({
  out: "./drizzle/migrations",
  schema: "./drizzle/src/db/schema.ts",
  dialect: "postgresql",
  dbCredentials: {
    url: Config.databaseUrl,
  },
});
