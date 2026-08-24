import path from "node:path";
import dotenv from "dotenv";

dotenv.config({
  path: path.resolve(__dirname, "../../../.env"),
});

const GlobalConfig = {
  databaseUrl: process.env.DATABASE_URL,
};

export default GlobalConfig;
