import "dotenv/config";

import GlobalConfig, { validateConfig } from "@wal/config";

const localConfig = validateConfig({
  PORT: process.env.PORT,
  clientId: process.env.CLIENT_ID,
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export default Config;
