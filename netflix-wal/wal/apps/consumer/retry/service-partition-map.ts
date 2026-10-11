import GlobalConfig, { TServicePartitionMapResponse } from "@wal/config";
import { createLogger } from "@wal/logger";

const log = createLogger("consumer:service-partition-map");

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
      `Service partition map refreshed with ${Object.keys(SERVICE_PARTITION_MAP).length} targets`,
    );
    return SERVICE_PARTITION_MAP;
  };
