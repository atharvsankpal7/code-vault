import { Admin } from "@platformatic/kafka";
import { zod } from "@wal/config";
import { createTopicSchema, updateTopicSchema } from "@wal/config/types";
import { createLogger } from "@wal/logger";
import { eq } from "drizzle-orm";
import db from "./control-plane-db";
import { kafkaTopic } from "./control-plane-db/schema";
import { reconsiler } from "./reconsiler";

const log = createLogger("control-plane:topic-controller");

// The database holds the desired topic state; the reconsiler applies it to kafka.
export const createKafkaTopic = async (
  admin: Admin,
  topicDetails: zod.output<typeof createTopicSchema>,
) => {
  const [topic] = await db
    .insert(kafkaTopic)
    .values({
      kafka_topic_name: topicDetails.topicName,
      partition_count: topicDetails.partitionCount,
      replication_factor: topicDetails.replicationCount,
      min_insync_replicas: topicDetails.min_insync_replicasCount,
      topic_production_type: topicDetails.topic_production_type,
      worker_wait_time_in_minutes: topicDetails.worker_wait_time_in_minutes,
      version: 1,
    })
    .returning();

  log.info(`Topic ${topic.kafka_topic_name} saved, reconciling`);
  await reconsiler(admin);
  return topic;
};

export const updateKafkaTopic = async (
  admin: Admin,
  topicName: string,
  topicDetails: zod.output<typeof updateTopicSchema>,
) => {
  const topic = await db.transaction(async (tx) => {
    // FOR UPDATE locks the row until commit, so concurrent patches to the same
    // topic queue up instead of both reading the same version.
    const [existing] = await tx
      .select({
        partitionCount: kafkaTopic.partition_count,
        version: kafkaTopic.version,
      })
      .from(kafkaTopic)
      .where(eq(kafkaTopic.kafka_topic_name, topicName))
      .for("update");

    if (!existing) {
      return null;
    }
    // Kafka only allows adding partitions, never removing them.
    if (
      topicDetails.partitionCount !== undefined &&
      topicDetails.partitionCount < existing.partitionCount
    ) {
      throw new Error(
        `partitionCount cannot go below ${existing.partitionCount} for ${topicName}`,
      );
    }

    const [updated] = await tx
      .update(kafkaTopic)
      .set({
        partition_count: topicDetails.partitionCount,
        min_insync_replicas: topicDetails.min_insync_replicasCount,
        topic_production_type: topicDetails.topic_production_type,
        worker_wait_time_in_minutes: topicDetails.worker_wait_time_in_minutes,
        version: existing.version + 1,
        reconciliation_status: "pending",
      })
      .where(eq(kafkaTopic.kafka_topic_name, topicName))
      .returning();

    return updated;
  });

  if (!topic) {
    return null;
  }

  // Reconcile after commit so the row lock is not held during kafka calls.
  log.info(
    `Topic ${topicName} updated to version ${topic.version}, reconciling`,
  );
  await reconsiler(admin);
  return topic;
};
