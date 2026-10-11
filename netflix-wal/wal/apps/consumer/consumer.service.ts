import type { Message } from "@platformatic/kafka";
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

// ---------- Error classification ----------

export type FailureKind = "transient" | "permanent";

export class DeliveryError extends Error {
  constructor(
    message: string,
    public readonly kind: FailureKind,
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

export const sendMessageToTarget = async (
  message: Message<string, string, string, string>,
  communicationType: DeliveryTargetCommunicationType,
  endpoint: TDeliveryTarget["endpoint"],
  timeout: TDeliveryTarget["timeout"],
  targetName: string,
  topic: string,
) => {
  const idempotencyKey = message.headers.get("idempotencyKey");

  if (communicationType === DeliveryTargetCommunicationType.http) {
    let response;
    try {
      response = await axios.post(
        `https://${endpoint}/${topic}`,
        { message: message.value, idempotencyKey },
        { headers: { idempotencyKey }, timeout },
      );
    } catch (err) {
      throw new DeliveryError(
        `HTTP delivery to ${targetName} failed for ${idempotencyKey}`,
        classifyHttpError(err),
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
      );
    }
    return;
  }

  if (communicationType === DeliveryTargetCommunicationType.pg) {
    try {
      await getPgPool(endpoint, timeout).query(
        "INSERT INTO write_ahead_log (message, topic, idempotencyKey) VALUES ($1, $2, $3)",
        [message.value, topic, idempotencyKey],
      );
    } catch (err) {
      // Duplicate idempotency key means an earlier attempt already succeeded
      if (err instanceof DatabaseError && err.code === "23505") return;
      throw new DeliveryError(
        `PG delivery to ${targetName} failed for ${idempotencyKey}`,
        classifyPgError(err),
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
          Body: message.value,
          ContentType: "application/json",
        }),
      );
    } catch (err) {
      throw new DeliveryError(
        `S3 delivery to ${targetName} failed for ${idempotencyKey}`,
        classifyS3Error(err),
        err,
      );
    }
    return;
  }

  throw new DeliveryError(
    `Unknown communication type: ${communicationType}`,
    "permanent",
  );
};

// ---------- Routing ----------

export const sendToDLQ = async (
  message: Message<string, string, string, string>,
  topic: string,
  error: DeliveryError,
): Promise<void> => {
  throw new Error("sendToDLQ not implemented");
};

export const sendToRetry = async (
  message: Message<string, string, string, string>,
  topic: string,
  error: DeliveryError,
): Promise<void> => {
  throw new Error("sendToRetry not implemented");
};

export const deliverMessage = async (
  message: Message<string, string, string, string>,
  target: TDeliveryTarget & {
    name: string;
    communicationType: DeliveryTargetCommunicationType;
  },
  topic: string,
) => {
  try {
    await sendMessageToTarget(
      message,
      target.communicationType,
      target.endpoint,
      target.timeout,
      target.name,
      topic,
    );
  } catch (err) {
    const deliveryError =
      err instanceof DeliveryError
        ? err
        : new DeliveryError(String(err), "transient", err);

    if (deliveryError.kind === "permanent") {
      await sendToDLQ(message, topic, deliveryError);
    } else {
      await sendToRetry(message, topic, deliveryError);
    }
  }
};
