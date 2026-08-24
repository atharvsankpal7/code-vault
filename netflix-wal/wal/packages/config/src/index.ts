import path from "node:path";
import dotenv from "dotenv";

dotenv.config({
  path: path.resolve(__dirname, "../../../.env"),
});

type EnvironmentConfig = Record<string, string | undefined>;

type ValidatedConfig<T extends EnvironmentConfig> = {
  [K in keyof T]: string;
};

export function validateConfig<T extends EnvironmentConfig>(
  config: T,
): ValidatedConfig<T> {
  for (const [key, value] of Object.entries(config)) {
    if (!value) {
      throw new Error(
        `Environment configuration error: "${key}" is not set correctly`,
      );
    }
  }

  return config as ValidatedConfig<T>;
}

const GlobalConfig = validateConfig({
  walDatabaseURI: process.env.DATABASE_URL,
});

export default GlobalConfig;
