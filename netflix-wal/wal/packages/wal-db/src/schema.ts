import {
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
} from "drizzle-orm/pg-core";

export const getCreatedAtUpdatedAtForTableGeneration = () => {
  return {
    created_at: timestamp("created_at").defaultNow().notNull(),
    updated_at: timestamp("updated_at")
      .notNull()
      .defaultNow()
      .$onUpdateFn(() => new Date()),
  };
};

const outboxStatus = pgEnum("outbox_status", [
  "pending",
  "in-progress",
  "done",
  "failed",
]);

export const wal_outbox = pgTable("wal_outbox", {
  id: uuid().primaryKey().defaultRandom(),
  status: outboxStatus().notNull(),
  topic_name: text("topic_name").notNull().unique(),
  message: text("message").notNull(), // in real life we would have types per topic in order to keep our system more robust
  error: jsonb(),
  workerId: text("worker_id"),
  ...getCreatedAtUpdatedAtForTableGeneration(),
});
