import {
  pgTable,
  integer,
  serial,
  text,
  boolean,
  timestamp,
  pgEnum,
} from "drizzle-orm/pg-core";
const getCreatedAtUpdatedAtForTableGeneration = () => {
  return {
    created_at: timestamp("created_at").defaultNow().notNull(),
    updated_at: timestamp("updated_at")
      .notNull()
      .defaultNow()
      .$onUpdateFn(() => new Date()),
  };
};
export const walNamespace = pgTable("wal_namespace", {
  id: serial("id").primaryKey(),
  namespace_name: text("namespace_name").notNull().unique(),
  is_enabled: boolean("is_enabled").default(false),
  version: serial("version").notNull(),
  ...getCreatedAtUpdatedAtForTableGeneration(),
});

const ReconciliationStatus = pgEnum("kafka_topic_reconciliation_status", [
  "pending",
  "done",
  "inprogress",
  "disabled",
  "error",
]);
export const kafkaTopic = pgTable("kafka_topic", {
  id: serial("id").primaryKey(),
  namespace_id: integer("namespace_id")
    .references(() => walNamespace.id)
    .notNull(),
  kafka_topic_name: text("kafka_topic_name").notNull(),
  partition_count: integer("partition_count").notNull(),
  replication_factor: integer("replication_factor").default(3).notNull(),
  min_insync_replicas: integer("min_insync_replicas").default(0).notNull(), // 0 in database = all in kafka
  last_reconciled_at: timestamp("last_reconciled_at"),
  reconciliation_status: ReconciliationStatus().default("pending").notNull(),
  ...getCreatedAtUpdatedAtForTableGeneration(),
});

const DeliveryTargetCommunicationType = pgEnum(
  "target_endpoint_communication_type",
  ["http", "pg", "s3"],
);
export const deliveryTarget = pgTable("delivery_target", {
  id: serial("id").primaryKey(),
  target_name: text("target_name").notNull(),
  endpoint_communication_type: DeliveryTargetCommunicationType().notNull(),
  endpoint: text("endpoint").notNull(),
  timeout: integer("timeout")
    .notNull()
    .default(15 * 1000),
  ...getCreatedAtUpdatedAtForTableGeneration(),
});

export const targetTopicSubscription = pgTable("target_topic_subscription", {
  id: serial("id").primaryKey(),
  kafka_topic_id: integer("kafka_topic_id")
    .references(() => kafkaTopic.id)
    .notNull(),
  delivery_target_id: integer("delivery_target_id")
    .references(() => deliveryTarget.id)
    .notNull(),
  is_enabled: boolean("is_enabled").default(false).notNull(),
  should_start_consumption_from_start: boolean(
    "should_start_consumption_from_start",
  ).notNull(), //true means start consumption of a topic from start, false means start consuming only new events
  ...getCreatedAtUpdatedAtForTableGeneration(),
});
