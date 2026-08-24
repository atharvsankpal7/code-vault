import GlobalConfig, { validateConfig } from "@wal/config";

const localConfig = validateConfig({
  PORT: process.env.PORT,
  databaseUrl: process.env.DATABASE_URL,
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export default Config;
