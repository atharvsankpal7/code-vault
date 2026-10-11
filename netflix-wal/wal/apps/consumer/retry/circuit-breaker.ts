import {
  BREAKER_BASE_PAUSE_MS,
  BREAKER_FAILURE_THRESHOLD,
  BREAKER_MAX_PAUSE_MS,
} from "./retry-config";

export enum BreakerState {
  closed = "closed",
  open = "open",
  halfOpen = "half-open",
}

interface Breaker {
  state: BreakerState;
  consecutiveFailures: number;
  pauseMs: number;
  pausedUntil: number;
}

// one breaker per retry partition. a retry partition only ever holds one
// delivery target and only one group member owns it, so in-memory state is
// accurate without sharing it across instances.
const breakers = new Map<string, Breaker>();

const getBreaker = (key: string): Breaker => {
  let breaker = breakers.get(key);
  if (!breaker) {
    breaker = {
      state: BreakerState.closed,
      consecutiveFailures: 0,
      pauseMs: BREAKER_BASE_PAUSE_MS,
      pausedUntil: 0,
    };
    breakers.set(key, breaker);
  }
  return breaker;
};

export const getBreakerState = (key: string) => getBreaker(key).state;

export const getPausedUntil = (key: string) => getBreaker(key).pausedUntil;

export const recordSuccess = (key: string) => {
  breakers.delete(key);
};

// returns true when the failure opened the breaker and the partition must pause
export const recordFailure = (key: string): boolean => {
  const breaker = getBreaker(key);

  if (breaker.state === BreakerState.halfOpen) {
    // probe failed, back off harder before the next probe
    breaker.pauseMs = Math.min(breaker.pauseMs * 2, BREAKER_MAX_PAUSE_MS);
    open(breaker);
    return true;
  }

  breaker.consecutiveFailures += 1;
  if (breaker.consecutiveFailures >= BREAKER_FAILURE_THRESHOLD) {
    open(breaker);
    return true;
  }
  return false;
};

// called when the pause timer fires, the next message is the probe
export const halfOpen = (key: string) => {
  const breaker = breakers.get(key);
  if (breaker?.state === BreakerState.open) {
    breaker.state = BreakerState.halfOpen;
  }
};

export const forgetBreaker = (key: string) => {
  breakers.delete(key);
};

const open = (breaker: Breaker) => {
  breaker.state = BreakerState.open;
  breaker.pausedUntil = Date.now() + breaker.pauseMs;
};
