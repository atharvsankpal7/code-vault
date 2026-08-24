import GlobalConfig, { validateConfig } from "@wal/config";

const localConfig = validateConfig({
  PORT: process.env.PORT,
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export default Config;
