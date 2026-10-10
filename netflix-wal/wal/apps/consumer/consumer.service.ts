import type { Message } from "@platformatic/kafka";
import type { TDeliveryTarget } from "@wal/config";
import { DeliveryTargetCommunicationType } from "@wal/config/types";

export const sendMessageToTarget = async (
  message: Message<string, string, string, string>,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
) => {
  if (communicationType === DeliveryTargetCommunicationType.http) {
    await fetch(`https://${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: message.value,
    });
  }
};
