import GlobalConfig, { validateConfig } from "@wal/config";
import path from "node:path";
import dotenv from "dotenv";
dotenv.config({
  path: path.resolve(__dirname, "./.env"),
});
const localConfig = validateConfig({
  PORT: process.env.PORT,
  controlPlaneDatabaseUrl: process.env.DATABASE_URL,
  kafkaClientId: process.env.KAFKA_CLIENT_ID,
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export default Config;
