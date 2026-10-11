import { Producer, stringSerializers, type Message } from "@platformatic/kafka";
import type { TDeliveryTarget } from "@wal/config";
import { DeliveryTargetCommunicationType } from "@wal/config/types";
import {
  PutObjectCommand,
  S3Client,
  S3ServiceException,
} from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import axios, { AxiosError } from "axios";
import { DatabaseError, Pool } from "pg";
import Config, { DLQ_TOPIC, RETRY_TOPIC_BY_STEP, TRetryStep } from "./config";
import { SERVICE_PARTITION_MAP } from "./infra-config";

// ---------- Error classification ----------

export type FailureKind = "transient" | "permanent";

export class DeliveryError extends Error {
  constructor(
    message: string,
    public readonly kind: FailureKind,
    public readonly target: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "DeliveryError";
  }
}

// Network-level errors: the target was unreachable or slow, so trying again may work
const TRANSIENT_NETWORK_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "ECONNABORTED",
  "EAI_AGAIN",
  "ENOTFOUND",
  "EPIPE",
  "EHOSTUNREACH",
  "ENETUNREACH",
]);

const isTransientHttpStatus = (status: number) =>
  status === 408 || status === 425 || status === 429 || status >= 500;

const classifyHttpError = (err: unknown): FailureKind => {
  if (err instanceof AxiosError) {
    // No response at all means a timeout, DNS failure, or refused connection
    if (!err.response) return "transient";
    return isTransientHttpStatus(err.response.status)
      ? "transient"
      : "permanent";
  }
  return "transient";
};

const classifyPgError = (err: unknown): FailureKind => {
  if (err instanceof DatabaseError && err.code) {
    const code = err.code;
    if (code.startsWith("08")) return "transient"; // connection exceptions
    if (code.startsWith("53")) return "transient"; // insufficient resources
    if (code === "40001" || code === "40P01") return "transient"; // serialization / deadlock
    if (code === "57014") return "transient"; // query_canceled (query_timeout)
    if (code.startsWith("57P")) return "transient"; // admin shutdown / crash / cannot connect now
    // 22xxx bad data, 23xxx constraint violations, 42xxx bad schema/SQL, 28xxx auth
    return "permanent";
  }
  const nodeCode = (err as NodeJS.ErrnoException)?.code;
  if (nodeCode && TRANSIENT_NETWORK_CODES.has(nodeCode)) return "transient";
  // pg's pool timeout throws a plain Error with this message
  if (err instanceof Error && /timeout/i.test(err.message)) return "transient";
  return "transient";
};

const TRANSIENT_S3_ERRORS = new Set([
  "SlowDown",
  "ThrottlingException",
  "RequestTimeout",
  "RequestTimeTooSkewed",
  "InternalError",
  "ServiceUnavailable",
  "TimeoutError",
]);

const classifyS3Error = (err: unknown): FailureKind => {
  if (err instanceof S3ServiceException) {
    if (err.$retryable) return "transient";
    if (TRANSIENT_S3_ERRORS.has(err.name)) return "transient";
    const status = err.$metadata?.httpStatusCode;
    if (status && isTransientHttpStatus(status)) return "transient";
    // NoSuchBucket, AccessDenied, InvalidAccessKeyId, etc.
    return "permanent";
  }
  const nodeCode = (err as NodeJS.ErrnoException)?.code;
  if (nodeCode && TRANSIENT_NETWORK_CODES.has(nodeCode)) return "transient";
  return "transient";
};

// ---------- Clients ----------

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

// ---------- Delivery ----------

// Delivers a raw value and throws a DeliveryError on failure. Shared by the
// platformatic consumer (via sendMessageToTarget) and the kafkajs retry consumer.
export const deliverToTarget = async (
  value: string,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
  targetName: string,
  topic: string,
  idempotencyKey: string,
): Promise<void> => {
  if (communicationType === DeliveryTargetCommunicationType.http) {
    let response;
    try {
      response = await axios.post(
        `https://${endpoint}/${topic}`,
        { message: value, idempotencyKey },
        { headers: { idempotencyKey }, timeout },
      );
    } catch (err) {
      throw new DeliveryError(
        `HTTP delivery to ${targetName} failed for ${idempotencyKey}`,
        classifyHttpError(err),
        targetName,
        err,
      );
    }
    // 2xx with an error body: the target received the message and rejected it
    if (response.data?.error) {
      throw new DeliveryError(
        `Target ${targetName} rejected message ${idempotencyKey}: ${JSON.stringify(
          response.data.error,
        )}`,
        "permanent",
        targetName,
      );
    }
    return;
  }

  if (communicationType === DeliveryTargetCommunicationType.pg) {
    try {
      await getPgPool(endpoint, timeout).query(
        "INSERT INTO write_ahead_log (message, topic, idempotencyKey) VALUES ($1, $2, $3)",
        [value, topic, idempotencyKey],
      );
    } catch (err) {
      // Duplicate idempotency key means an earlier attempt already succeeded
      if (err instanceof DatabaseError && err.code === "23505") return;
      throw new DeliveryError(
        `PG delivery to ${targetName} failed for ${idempotencyKey}`,
        classifyPgError(err),
        targetName,
        err,
      );
    }
    return;
  }

  if (communicationType === DeliveryTargetCommunicationType.s3) {
    try {
      await getS3Client(timeout).send(
        new PutObjectCommand({
          Bucket: endpoint,
          Key: `${topic}/${idempotencyKey}.json`,
          Body: value,
          ContentType: "application/json",
        }),
      );
    } catch (err) {
      throw new DeliveryError(
        `S3 delivery to ${targetName} failed for ${idempotencyKey}`,
        classifyS3Error(err),
        targetName,
        err,
      );
    }
    return;
  }

  throw new DeliveryError(
    `Unknown communication type: ${communicationType}`,
    "permanent",
    targetName,
  );
};

export const sendMessageToTarget = async (
  message: Message<string, string, string, string>,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
  targetName: string,
  topic: string,
  idempotencyKey: string,
) => {
  try {
    await deliverToTarget(
      message.value,
      communicationType,
      endpoint,
      timeout,
      targetName,
      topic,
      idempotencyKey,
    );
  } catch (err) {
    if (err instanceof DeliveryError) {
      if (err.kind === "permanent") {
        await sendToDLQ(message, err);
        message.commit();
      }
    } else {
      throw err;
    }
  }
};

// ---------- Routing ----------
export const producer = new Producer({
  clientId: Config.serviceSpecificClientId,
  bootstrapBrokers: Config.kafkaBrokers.split(","),
  serializers: stringSerializers,
});
// Shared by every consumer that writes to the DLQ, so the header format stays the same.
// The caller serializes its own metadata (if any) and passes it in.
export type TDLQSource = {
  value: string;
  headers: Record<string, string>;
  originalTopic: string;
  originalOffset: string;
  originalMetadata?: string;
  error: DeliveryError;
};

export const buildDLQMessage = (source: TDLQSource) => ({
  value: source.value,
  headers: {
    ...source.headers,
    target: source.error.target,
    originalTopic: source.originalTopic,
    originalOffset: source.originalOffset,
    ...(source.originalMetadata !== undefined && {
      originalMetadata: source.originalMetadata,
    }),
    error: JSON.stringify({
      error: source.error,
    }),
  },
});

export const sendToDLQ = async (
  message: Message<string, string, string, string>,
  error: DeliveryError,
) => {
  return await producer.send({
    messages: [
      {
        topic: DLQ_TOPIC.name,
        ...buildDLQMessage({
          value: message.value,
          headers: Object.fromEntries(message.headers),
          originalTopic: message.topic,
          originalOffset: String(message.offset),
          originalMetadata: JSON.stringify(message.metadata),
          error,
        }),
      },
    ],
  });
};

export const sendToRetry = async (
  retryStep: TRetryStep,
  message: Message<string, string, string, string>,
  error: DeliveryError,
): Promise<void> => {
  const partition = SERVICE_PARTITION_MAP[error.target];
  // no partition assigned to this target, so the retry can't be routed
  if (partition === undefined) {
    await sendToDLQ(message, error);
    return;
  }

  await producer.send({
    messages: [
      {
        topic: RETRY_TOPIC_BY_STEP[retryStep].name,
        value: message.value,
        headers: {
          ...Object.fromEntries(message.headers),
          target: error.target,
          originalTopic: message.topic,
          originalOffset: String(message.offset),
          originalMetadata: JSON.stringify(message.metadata),
          error: JSON.stringify({
            error,
          }),
        },
        partition,
      },
    ],
  });
};
