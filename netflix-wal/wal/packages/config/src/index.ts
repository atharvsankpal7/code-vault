import "dotenv/config";

const Config = {
  databaseUrl:
    process.env.DATABASE_URL ??
    "postgresql://postgres:postgres@localhost:5432/wal",
} as const;

export default Config;
