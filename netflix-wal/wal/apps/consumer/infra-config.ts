import GlobalConfig, {
  TKafkaConsumerTopicMapResponse,
  TServicePartitionMapResponse,
} from "@wal/config";
import Config from "./config";
import { createLogger } from "@wal/logger";

const log = createLogger("consumer:kafka-config");

export let KAKFA_CONFIG: TKafkaConsumerTopicMapResponse = {};

export const refreshConsumerTopicMap =
  async (): Promise<TKafkaConsumerTopicMapResponse> => {
    log.debug("Refreshing topic map from control-plane");
    const response = await fetch(
      `${Config.controlPlaneUrl}/get-consumer-topic-map`,
    );
    if (!response.ok) {
      log.error(`Control-plane returned ${response.status} for topic map`);
    }
    const data: { topicMap: TKafkaConsumerTopicMapResponse } =
      await response.json();
    KAKFA_CONFIG = data.topicMap ?? {};
    log.info(
      `Topic map refreshed with ${Object.keys(KAKFA_CONFIG).length} topics`,
    );
    return KAKFA_CONFIG;
  };

export let SERVICE_PARTITION_MAP: TServicePartitionMapResponse = {};
export const refreshServicePartitionMap =
  async (): Promise<TServicePartitionMapResponse> => {
    log.debug("Refreshing service partition map from control-plane");
    const response = await fetch(
      `${GlobalConfig.controlPlaneUrl}/get-service-partition-map`,
    );
    if (!response.ok) {
      // keep serving the last known map instead of dropping every target
      log.error(
        `Control-plane returned ${response.status} for service partition map`,
      );
      return SERVICE_PARTITION_MAP;
    }
    const data: { servicePartitionMap: TServicePartitionMapResponse } =
      await response.json();
    SERVICE_PARTITION_MAP = data.servicePartitionMap ?? {};
    log.info(
      `Service partition map refreshed with ${
        Object.keys(SERVICE_PARTITION_MAP).length
      } targets`,
    );
    return SERVICE_PARTITION_MAP;
  };
