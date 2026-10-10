import { TopicOperationType, zod } from "@wal/config";

export const createTopicSchema = zod.object({
  topicName: zod.string(),
  partitionCount: zod.coerce.number(),
  replicationCount: zod.coerce.number(),
  min_insync_replicasCount: zod.coerce.number(),
  topic_production_type: zod.enum(TopicOperationType),
  worker_wait_time_in_minutes: zod.coerce.number().optional(),
});

export type TCreateTopicDetails = zod.output<typeof createTopicSchema>;

// Kafka cannot change a topic's replication factor in place, and the name is
// taken from the route, so neither can be patched.
export const updateTopicSchema = createTopicSchema
  .omit({ topicName: true, replicationCount: true })
  .partial();

export enum DeliveryTargetCommunicationType {
  http = "http",
  pg = "pg",
  s3 = "s3",
}
