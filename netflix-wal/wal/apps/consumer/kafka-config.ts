import { TKafkaConsumerTopicMapResponse } from "@wal/config";
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
