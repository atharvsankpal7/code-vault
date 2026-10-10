import type { Message } from "@platformatic/kafka";
import type { TDeliveryTarget } from "@wal/config";
import { DeliveryTargetCommunicationType } from "@wal/config/types";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import axios from "axios";
import { Pool } from "pg";

const s3Clients = new Map<number, S3Client>();

const getS3Client = (timeout: number): S3Client => {
  let client = s3Clients.get(timeout);
  if (!client) {
    client = new S3Client({
      requestHandler: new NodeHttpHandler({
        connectionTimeout: timeout,
        requestTimeout: timeout,
      }),
    });
    s3Clients.set(timeout, client);
  }
  return client;
};

const pgPools = new Map<string, Pool>();

const getPgPool = (endpoint: string, timeout: number): Pool => {
  let pool = pgPools.get(endpoint);
  if (!pool) {
    pool = new Pool({
      connectionString: endpoint,
      connectionTimeoutMillis: timeout,
      query_timeout: timeout,
    });
    pgPools.set(endpoint, pool);
  }
  return pool;
};

export const sendMessageToTarget = async (
  message: Message<string, string, string, string>,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
  targetName: string,
  topic: string,
) => {
  if (communicationType === DeliveryTargetCommunicationType.http) {
    const response = await axios.post(
      `https://${endpoint}/${topic}`,
      { message: message.value },
      {
        timeout: timeout,
      },
    );

    if (!(200 <= response.status && response.status < 300)) {
      throw new Error(
        `failed to deliver message to ${targetName} with offset :[${message.offset}] from topic: [${topic}]`,
      );
    }

    if (response.data?.error) {
      throw new Error(
        `failed to deliver message to ${targetName} with offset :[${message.offset}] from topic: [${topic}]`,
      );
    }
  }
  if (communicationType === DeliveryTargetCommunicationType.pg) {
    const pool = getPgPool(endpoint, timeout);
    await pool.query(
      "INSERT INTO write_ahead_log (message, topic) VALUES ($1, $2)",
      [message.value, topic],
    );
  }
  if (communicationType === DeliveryTargetCommunicationType.s3) {
    await getS3Client(timeout).send(
      new PutObjectCommand({
        Bucket: endpoint,
        Key: `${topic}/${message.partition}-${message.offset}.json`,
        Body: message.value,
        ContentType: "application/json",
      }),
    );
  }
};
