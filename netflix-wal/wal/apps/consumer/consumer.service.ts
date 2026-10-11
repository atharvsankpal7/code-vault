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

export const getIdempotencyKey = (
  message: Message<string, string, string, string>,
  targetName: string,
  topic: string,
) =>
  message.headers.get("idempotencyKey") ??
  `${targetName}-${topic}-${message.partition}-${message.offset}`;

// pg error classes that mean the message itself is bad: 22 = data exception,
// 23 = integrity constraint violation, 42 = syntax error or access rule violation
const nonRetryablePgErrorClasses = new Set(["22", "23", "42"]);

// true when sending the same message again could succeed (5xx, timeouts,
// connection errors, 408, 429), false when it never will (other 4xx, bad data)
export const isRetryableError = (err: unknown): boolean => {
  if (axios.isAxiosError(err)) {
    const status = err.response?.status;
    if (status === undefined) {
      return true;
    }
    return status >= 500 || status === 408 || status === 429;
  }

  const pgCode = (err as { code?: unknown })?.code;
  if (typeof pgCode === "string" && pgCode.length === 5) {
    return !nonRetryablePgErrorClasses.has(pgCode.slice(0, 2));
  }

  const s3Status = (err as { $metadata?: { httpStatusCode?: number } })
    ?.$metadata?.httpStatusCode;
  if (s3Status !== undefined) {
    return s3Status >= 500 || s3Status === 408 || s3Status === 429;
  }

  return true;
};

export const sendMessageToTarget = async (
  message: Message<string, string, string, string>,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
  targetName: string,
  topic: string,
) =>
  deliverToTarget(
    message.value,
    getIdempotencyKey(message, targetName, topic),
    communicationType,
    endpoint,
    timeout,
    topic,
  );

// client agnostic delivery, shared by the main consumer and the retry consumer
export const deliverToTarget = async (
  value: string,
  idempotencyKey: string,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
  topic: string,
) => {
  if (communicationType === DeliveryTargetCommunicationType.http) {
    const response = await axios.post(
      `https://${endpoint}/${topic}`,
      {
        message: value,
        idempotencyKey,
      },
      { headers: { idempotencyKey }, timeout: timeout },
    );

    if (!(200 <= response.status && response.status < 300)) {
      throw new Error(`failed to deliver message ${idempotencyKey}`);
    }

    if (response.data?.error) {
      throw new Error(`failed to deliver message ${idempotencyKey}`);
    }
  }
  if (communicationType === DeliveryTargetCommunicationType.pg) {
    const pool = getPgPool(endpoint, timeout);
    await pool.query(
      "INSERT INTO write_ahead_log (message, topic, idempotencyKey) VALUES ($1, $2, $3) ON CONFLICT (idempotencyKey) DO NOTHING",
      [value, topic, idempotencyKey],
    );
  }
  if (communicationType === DeliveryTargetCommunicationType.s3) {
    await getS3Client(timeout).send(
      new PutObjectCommand({
        Bucket: endpoint,
        Key: `${topic}/${idempotencyKey}.json`,
        Body: value,
        ContentType: "application/json",
      }),
    );
  }
};
