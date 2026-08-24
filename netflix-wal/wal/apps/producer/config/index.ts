import GlobalConfig from "@wal/config";

let Config = {
  PORT: process.env.PORT,
};

Object.entries(Config).forEach(([key, value]) => {
  if (!value) {
    console.error(
      `Environment configuration error: "${key}" is not set correctly`,
    );
    process.exit(1);
  }
});

let localConfig: {} = Config as Record<keyof typeof Config, string>;
Config = {
  ...Config,
  ...GlobalConfig,
  ...localConfig,
};
export default Config;
