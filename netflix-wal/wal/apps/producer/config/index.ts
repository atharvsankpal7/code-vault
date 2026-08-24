import "dotenv/config";

import GlobalConfig, { validateConfig } from "@wal/config";

const localConfig = validateConfig({
  PORT: process.env.PORT,
  walDatabaseUrl: process.env.WAL_DATABASE_URL,
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export default Config;
