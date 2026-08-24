import "dotenv/config";
import { defineConfig } from "drizzle-kit";
import Config from "./config";

export default defineConfig({
  schema: "./control-plane-db/schema",

  out: "./control-plane-db/migrations",

  dialect: "postgresql",

  dbCredentials: {
    url: Config.databaseUrl,
  },

  strict: true,
  verbose: true,
});
