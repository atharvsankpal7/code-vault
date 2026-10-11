import { createLogger } from "@wal/logger";
import { refreshConsumerTopicMap } from "./kafka-config";
import { SERVICE_PARTITION_MAP_REFRESH_MS } from "./retry/retry-config";
import { startRetryConsumer } from "./retry/retry.consumer";
import { refreshServicePartitionMap } from "./retry/service-partition-map";

const log = createLogger("consumer:retry-main");

const refreshMaps = () =>
  Promise.all([refreshConsumerTopicMap(), refreshServicePartitionMap()]);

const main = async () => {
  await refreshMaps();
  setInterval(() => {
    refreshMaps().catch((err) => log.error("Failed to refresh maps", err));
  }, SERVICE_PARTITION_MAP_REFRESH_MS);

  await startRetryConsumer();
};

main().catch((err) => {
  log.error("Retry consumer crashed", err);
  process.exit(1);
});
