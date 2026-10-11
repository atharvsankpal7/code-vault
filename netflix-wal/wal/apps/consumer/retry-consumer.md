## 1. Overall flow

```
                 ┌────────────────────┐
                 │   Control Plane DB │
                 │ - topic map        │
                 │ - service→partition│
                 └─────────┬──────────┘
                           │ load at startup + refresh every N min
          ┌────────────────┼──────────────────────┐
          ▼                ▼                      ▼
   ┌─────────────┐  ┌──────────────┐        ┌──────────────┐
   │Main Consumer│  │Retry Consumer│  ...   │Retry Consumer│   (group per tier,
   └──────┬──────┘  └──────┬───────┘        └──────────────┘    auto-assigned)
          │                │
 topic X ─┤                │
          ▼                ▼
   call A, B, C     call target-endpoint
          │
   ┌──────┴────────────┬───────────────┐
   ▼                   ▼               ▼
 success            4xx error        5xx/timeout
 (nothing)         ──► DLQ          ──► retry-10s ──► retry-1m ──► retry-5m ──► DLQ
                                   (partition = serviceMap[target])
```

## 2. Main consumer

```
on message from topic X:
    for each endpoint in config[topic X]:
        result = call(endpoint, payload, idemKey = msg.key + endpoint)

        if result == success:            continue
        if result is non-retryable (4xx): produceToDLQ(msg, endpoint, error)
        if result is retryable (5xx/timeout/429/408):
            produceRetry(tier = "retry-10s", msg, endpoint, attempt = 1)

    await all produces acked
    commit(offset)
```

## 3. Producing a retry

```
produceRetry(tier, msg, endpoint, attempt):
    partition = serviceMap[endpoint]          # explicit, no hashing
    if partition is undefined: produceToDLQ(...) + alert; return

    headers = {
        target-endpoint : endpoint,
        retry-attempt   : attempt,
        due-at          : now + tierDelay[tier] + jitter(±10-20%),
        idempotency-key : msg.key + endpoint,
        original-topic / partition / offset,
        first-failed-at, last-error
    }
    produce(tier, partition, msg.payload, headers)

nextTier: retry-10s → retry-1m → retry-5m → DLQ
```

## 4. Retry consumer (one group per tier)

```
state (in memory, per partition):
    breaker[p] = { state: CLOSED | OPEN | HALF_OPEN,
                   consecutiveFailures, pausedUntil, backoff }
    timers[p]

on batch(topic, partition p, messages):
    for msg in messages (in order):

        # --- 1. message backoff ---
        if msg.dueAt > now:
            pause(p); seek(p, msg.offset)
            schedule resume at msg.dueAt
            return

        # --- 2. call ---
        result = call(serviceUrl[msg.target], msg.payload, msg.idemKey)

        # --- 3. success ---
        if result == success:
            breaker[p] = CLOSED, failures = 0, backoff = base
            commit(msg.offset)
            continue

        # --- 4. permanent failure ---
        if result is 4xx:
            await produceToDLQ(msg); commit(msg.offset)
            continue                      # does NOT count against breaker

        # --- 5. retryable failure ---
        if breaker[p].state == HALF_OPEN:          # probe failed
            openBreaker(p, msg.offset, backoff * 2)  # don't commit, don't forward
            return

        breaker[p].consecutiveFailures++

        if consecutiveFailures >= THRESHOLD (5-10):
            openBreaker(p, msg.offset, backoff)    # don't commit, don't forward
            return

        # normal retry path
        if msg.attempt >= MAX: await produceToDLQ(msg)
        else:                  await produceRetry(nextTier, msg, msg.target, attempt+1)
        commit(msg.offset)
```

## 5. Circuit breaker

```
openBreaker(p, offset, backoff):
    pause(p)
    seek(p, offset)                       # replay this msg later, no attempt burned
    breaker[p] = OPEN, pausedUntil = now + min(backoff, MAX_BACKOFF)
    timers[p] = setTimeout(halfOpen(p), pausedUntil - now)

halfOpen(p):
    try:
        breaker[p].state = HALF_OPEN
        resume(p)                         # next message = probe
    catch:
        log + retry resume                # never leave it stuck
```

## 6. Watchdog (every 30–60s)

```
for p in consumer.paused():
    if no breaker/due record for p           → resume(p), warn
    if now > pausedUntil + grace             → resume(p), warn
    if paused longer than 30 min             → alert "service-x down"
```

## 7. Rebalance handling

```
on partitions revoked:
    clear timers[p], delete breaker[p]

on partitions assigned:
    start fresh (CLOSED, unpaused), consume from committed offset
```

## 8. Config refresh

```
every N minutes:
    newMap = controlPlane.getServicePartitionMap()
    if newMap.version != current.version:
        swap serviceMap atomically, log version
```

###### Decisions recap

| Decision            | Choice                                                                               |
| ------------------- | ------------------------------------------------------------------------------------ |
| Retry storage       | Kafka tier topics:`retry-10s`, `retry-1m`, `retry-5m`, plus `dlq`                    |
| Endpoint isolation  | Explicit service → partition map (64 partitions), in the control plane               |
| Consumer assignment | Consumer group with automatic assignment (no custom assigner)                        |
| Message delay       | `due-at` header; pause, seek, and resume when due                                    |
| Endpoint outage     | Per-partition breaker: trip after 5–10 consecutive failures, timer → half-open probe |
| Outage messages     | Seek back; don't commit or forward, so no attempts are burned                        |
| Non-retryable       | 4xx goes straight to the DLQ and doesn't count for the breaker                       |
| Safety              | Commit only after the produce is acked; idempotency key = msg key + endpoint         |
| Stuck pause         | Watchdog plus alerts                                                                 |
