import { TKafkaTopicMapResponse } from "@wal/config";
import Config from "./config";

export let KAKFA_CONFIG: TKafkaTopicMapResponse = {};

export const refreshTopicMap = async (): Promise<TKafkaTopicMapResponse> => {
  const response = await fetch(`${Config.controlPlaneUrl}/get-topic-map`);
  const data: { topicMap: TKafkaTopicMapResponse } = await response.json();
  KAKFA_CONFIG = data.topicMap ?? {};
  return KAKFA_CONFIG;
};
