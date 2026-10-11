export interface RetryTier {
  topic: string;
  delayMs: number;
}

// every message in a tier waits the same delay, so the head of a partition is
// always the next message due and a consumer never has to scan past it
export const RETRY_TIERS: RetryTier[] = [
  { topic: "wal-retry-10s", delayMs: 10 * 1000 },
  { topic: "wal-retry-1m", delayMs: 60 * 1000 },
  { topic: "wal-retry-5m", delayMs: 5 * 60 * 1000 },
];

export const DLQ_TOPIC = "wal-dlq";

// +-20% of the tier delay, keeps retries of one outage from landing together
export const JITTER_RATIO = 0.2;

// circuit breaker, per retry partition (= per delivery target)
export const BREAKER_FAILURE_THRESHOLD = 10;
export const BREAKER_BASE_PAUSE_MS = 10 * 60 * 1000;
export const BREAKER_MAX_PAUSE_MS = 60 * 60 * 1000;

export const WATCHDOG_INTERVAL_MS = 30 * 1000;
export const SERVICE_PARTITION_MAP_REFRESH_MS = 5 * 60 * 1000;

export const RetryHeaders = {
  targetName: "targetName",
  retryAttempt: "retryAttempt",
  dueAt: "dueAt",
  idempotencyKey: "idempotencyKey",
  originalTopic: "originalTopic",
  originalPartition: "originalPartition",
  originalOffset: "originalOffset",
  firstFailedAt: "firstFailedAt",
  lastError: "lastError",
} as const;
