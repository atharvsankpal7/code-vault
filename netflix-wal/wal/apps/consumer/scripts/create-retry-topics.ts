import { Admin } from "@platformatic/kafka";
import GlobalConfig from "@wal/config";
import { createLogger } from "@wal/logger";

const log = createLogger("consumer:create-retry-topics");

const REPLICATION_FACTOR = 3;
const MIN_INSYNC_REPLICAS = 2;
const FOURTEEN_DAYS_MS = String(14 * 24 * 60 * 60 * 1000);
const RETAIN_FOREVER = "-1";

type TRetryTopic = {
  name: string;
  partitions: number;
  retentionMs: string;
};

const RETRY_TOPICS: TRetryTopic[] = [
  { name: "retry-10s", partitions: 64, retentionMs: FOURTEEN_DAYS_MS },
  { name: "retry-1m", partitions: 64, retentionMs: FOURTEEN_DAYS_MS },
  { name: "retry-5m", partitions: 64, retentionMs: FOURTEEN_DAYS_MS },
  { name: "dlq", partitions: 12, retentionMs: RETAIN_FOREVER },
];

const run = async () => {
  const admin = new Admin({
    clientId: "create-retry-topics",
    bootstrapBrokers: GlobalConfig.kafkaBrokers.split(","),
  });

  try {
    const existingTopics = new Set(await admin.listTopics());
    const missingTopics = RETRY_TOPICS.filter(
      (topic) => !existingTopics.has(topic.name),
    );

    if (missingTopics.length === 0) {
      log.info("All retry topics already exist, nothing to create");
      return;
    }

    for (const topic of missingTopics) {
      log.info(`Creating ${topic.name} with ${topic.partitions} partitions`);
      await admin.createTopics({
        topics: [topic.name],
        partitions: topic.partitions,
        replicas: REPLICATION_FACTOR,
        configs: [
          { name: "min.insync.replicas", value: String(MIN_INSYNC_REPLICAS) },
          { name: "retention.ms", value: topic.retentionMs },
        ],
      });
    }

    log.info(`Created ${missingTopics.length} retry topic(s)`);
  } finally {
    await admin.close();
  }
};

run().catch((err) => {
  log.error("Failed to create retry topics", err);
  process.exitCode = 1;
});
