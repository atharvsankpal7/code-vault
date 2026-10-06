import "dotenv/config";
import GlobalConfig, { validateConfig } from "@wal/config";
import { uuidv7 } from "uuidv7";

const consumerServices = validateConfig({
  SPECIFIC_CONSUMER_SERVICE: process.env.SPECIFIC_CONSUMER_SERVICE,
  SHARED_CONSUMER_SERVICES: process.env.SHARED_CONSUMER_SERVICES,
});

const serviceSpecificGroupId = `service-${consumerServices.SPECIFIC_CONSUMER_SERVICE}-consumer-`;
const sharedConsumerGroupId = `service-${consumerServices.SHARED_CONSUMER_SERVICES.split(
  ",",
)
  .map((service) => service)
  .join("-")}-consumer`;
const localConfig = validateConfig({
  PORT: process.env.PORT,
  serviceSpecificGroupId,
  sharedConsumerGroupId,
  serviceSpecificClientId: serviceSpecificGroupId + uuidv7(),
  sharedConsumerClientId: sharedConsumerGroupId + uuidv7(),
});

const Config = {
  ...GlobalConfig,
  ...localConfig,
} as const;

export default Config;
