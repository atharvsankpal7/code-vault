import GlobalConfig from "@wal/config";
import "dotenv/config";
const Config = {
  ...GlobalConfig,
  PORT: process.env.PORT,
} as const;

export default Config;
