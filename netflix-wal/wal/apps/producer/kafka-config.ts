import { TKafkaTopicMapResponse } from "@wal/config";
import Config from "./config";
import { createLogger } from "@wal/logger";

const log = createLogger("producer:kafka-config");

export let KAKFA_CONFIG: TKafkaTopicMapResponse = {};

export const refreshTopicMap = async (): Promise<TKafkaTopicMapResponse> => {
  log.debug("Refreshing topic map from control-plane");
  const response = await fetch(`${Config.controlPlaneUrl}/get-topic-map`);
  if (!response.ok) {
    log.error(`Control-plane returned ${response.status} for topic map`);
  }
  const data: { topicMap: TKafkaTopicMapResponse } = await response.json();
  KAKFA_CONFIG = data.topicMap ?? {};
  log.info(`Topic map refreshed with ${Object.keys(KAKFA_CONFIG).length} topics`);
  return KAKFA_CONFIG;
};
