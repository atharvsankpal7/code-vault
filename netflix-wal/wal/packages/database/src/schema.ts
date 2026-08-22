import { integer, jsonb, pgTable, varchar } from "drizzle-orm/pg-core";

export const wal = pgTable("wal", {
  id: integer().primaryKey().generatedAlwaysAsIdentity(),
  producer: varchar("producer", { length: 255 }).notNull(),
  consumer: varchar("consumer", { length: 255 }).notNull(),
  data: jsonb("data").notNull(),
  retryLeft: integer("retryLeft").notNull().default(3),
});
