import 'dotenv/config';


const Config = {
    port : process.env.PORT ?? 8081,
    databaseUrl: process.env.DATABASE_URL ?? "postgresql://postgres:postgres@localhost:5432/wal",
} as const;


export default Config;