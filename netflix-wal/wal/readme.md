# WAL Architecture Summary

This project models a Write-Ahead Log (WAL) platform inspired by Netflix's data platform architecture.

https://netflixtechblog.com/building-a-resilient-data-platform-with-write-ahead-log-at-netflix-127b6712359a

## High-level flow

```text
Client application
        |
        v
WAL API / Producer
        |
        v
Kafka WAL topic
        |
        v
WAL Consumer Group
        |
        v
Target database, cache, queue, or service
```

The producer writes the event to Kafka first. The consumer later applies the event to the target. This decouples incoming writes from downstream processing and allows retry and replay when a target is unavailable.

## Kafka cluster

- A Kafka broker stores topic partitions and handles producer/consumer requests.
- A partition is an ordered log of messages.
- A partition has one leader broker and possibly multiple follower replicas.
- Producers send records to the partition leader; the leader replicates them to followers.
- KRaft controllers manage Kafka metadata, including brokers, topics, partition assignments, replicas, and leader elections.
- For fault tolerance, Kafka commonly uses three controllers. One is active and the others replicate metadata and can take over.
- A Kafka process may run as both a broker and a controller in development or combined deployments.

## Producer

The WAL producer/API:

1. Receives a request from a client application.
2. Validates and normalizes the payload.
3. Adds an event envelope such as event ID, event type, namespace, timestamp, and schema version.
4. Publishes the complete event to Kafka.
5. Returns success only after Kafka provides the required durability acknowledgement.

Kafka should normally contain the complete event, not only a database ID. Storing only an ID would require an additional database read for every message and would make replay dependent on the database record still existing.

## Consumer group

The WAL consumer reads Kafka and applies events to the configured target.

Multiple instances of the same consumer use the same `groupId`:

```text
Consumer A --\
Consumer B ----> group: orders-writer --> Kafka topic: orders
Consumer C --/
```

Kafka automatically assigns partitions among group members. If one consumer fails, Kafka detects missing heartbeats and reassigns its partitions to another member. No load balancer is required between Kafka and the consumers.

The maximum active consumer parallelism is limited by the number of partitions. Extra consumers remain idle until more partitions are available.

Consumer processing is generally at-least-once:

1. Read an event.
2. Apply it to the target.
3. Commit the Kafka offset only after successful processing.

If the consumer fails before committing, the event may be processed again. Target operations should therefore be idempotent or use an inbox/deduplication mechanism.

## Control plane

The control plane stores durable configuration for each WAL namespace. A namespace is a logical configuration for a use case.

Configuration can include:

- Kafka or SQS as the underlying queue
- Kafka topic names
- Consumer group or consumer stack
- Target database, cache, queue, or service
- Retry and backoff policy
- Dead-letter queue/topic
- Ordering or partitioning requirements
- Regional or cross-region targets

The configuration should be stored in durable storage, such as PostgreSQL. Consumers can load it at startup and keep a local in-memory cache. Configuration changes can be detected through polling, version checks, or notifications.

The control plane does not process every event and does not need to track every live consumer instance. Kafka tracks consumer-group membership and performs partition assignment. Deployment infrastructure starts and scales the consumer instances.

## Scaling and deployment

The same consumer image can run as multiple instances:

```text
Consumer image
    |
    +-- instance 1: group orders-writer
    +-- instance 2: group orders-writer
    +-- instance 3: group orders-writer
```

Kubernetes or another deployment platform manages:

- Container images
- Number of replicas
- Restarts
- Health checks
- Resource limits
- Autoscaling

Kafka manages the distribution of topic partitions among the running consumer instances.

At Netflix, WAL producer and consumer groups are scaled independently based on operational signals such as CPU and network thresholds. WAL is deployed through existing Data Gateway infrastructure, which supplies runtime and deployment capabilities.

## Retries and DLQ

When a consumer cannot deliver an event to its target:

```text
Kafka event
    |
    +-- attempt 1 fails --> retry/backoff
    +-- attempt 2 fails --> retry/backoff
    +-- maximum attempts --> dead-letter topic
```

The DLQ preserves messages that repeatedly fail so they can be inspected and replayed later. Retry count, backoff, and DLQ destination belong in namespace configuration.

## Two ways services can integrate

### Direct Kafka consumption

```text
Service Z --> Kafka topic A
```

Service Z owns its Kafka consumer, deployment, group ID, subscription, and processing logic. The WAL control plane does not need to know its HTTP endpoint.

### WAL-managed delivery

```text
Kafka topic A --> WAL consumer --> Service Z endpoint
```

Service Z exposes an endpoint and registers it as a WAL target. The WAL control plane stores the topic-to-target mapping, authentication details, retry policy, DLQ, and delivery contract. Service Z's deployment remains the responsibility of its own deployment platform.

Netflix WAL is primarily an abstraction like the second model: application teams use a simple WAL API while the platform manages queues, retries, targets, and operational complexity.

## Core principle

```text
Control plane: what should happen?
Kafka:         store and distribute events
Consumer:      perform the work
Deployment:    keep enough worker instances running
Target:        apply the business-side mutation
```

## Implementation plan

### Storage responsibilities

The platform uses a control-plane PostgreSQL database, a separate durable WAL
store, and Kafka. The durable WAL store is initially implemented with PostgreSQL
behind the shared `@wal/wal-db` package and can later be replaced by a distributed
key-value store.

```text
Control-plane PostgreSQL
  - WAL namespaces
  - desired Kafka topic configuration
  - delivery targets and retry policies
  - configuration versions
  - reconciliation status and errors

Durable WAL store
  - advanced multi-table or multi-partition operations
  - ordered mutation content and operation state
  - completion-marker outbox

Kafka
  - original WAL events
  - lightweight completion markers for advanced operations
  - durable retry records
  - dead-letter records
  - consumer offsets
```

Ordinary namespaces publish their complete event to Kafka. Advanced namespaces
first persist their content, state, and ordering sequence to the durable WAL store
in one transaction. That transaction also creates an outbox entry. An outbox
publisher forwards a lightweight completion marker to Kafka, and the consumer uses
the marker's operation ID to reconstruct the ordered mutations from durable
storage. This workflow remains at-least-once and therefore uses idempotent marker
publication and target operations.

### Control plane

The control plane owns the configuration API and runs an idempotent reconciliation
loop. An API change first updates the desired state in PostgreSQL and increments
the configuration generation. The reconciler then compares that state with Kafka
and safely moves Kafka toward the requested configuration.

The first version will support:

- Creating missing topics.
- Increasing partition counts, but never reducing them.
- Applying mutable topic settings such as retention.
- Rejecting or separately handling destructive and unsupported changes.
- Recording the observed generation, reconciliation status, last attempt, and
  last error.
- Periodic reconciliation plus a manually triggered reconciliation endpoint.
- Database locking so multiple control-plane replicas do not process the same
  configuration concurrently.

### Runtime configuration

The producer and consumer keep validated configuration snapshots in memory. They
poll role-specific control-plane endpoints using HTTP conditional requests:

```text
GET /v1/runtime-config/producer
GET /v1/runtime-config/consumer
```

Each response includes a monotonically increasing version and an `ETag`. A worker
sends `If-None-Match` on its next poll and receives `304 Not Modified` when its
configuration is current. PostgreSQL stores only the latest configuration, so
workers do not request historical snapshots.

If a refresh fails, a worker continues using its last valid snapshot. Target URL
and retry-policy changes can be swapped in memory. Changes to a topic, consumer
group, subscription, or Kafka credentials require a graceful restart of the
affected consumer.

### Producer

The producer will:

1. Resolve the namespace through its cached runtime configuration.
2. Validate and normalize the request.
3. Create an envelope containing an event ID, namespace, timestamp, schema
   version, and payload.
4. Publish the complete event to the configured Kafka topic.
5. Return success only after Kafka acknowledges the configured durability level.

### Consumer and targets

Kafka does not deliver records directly to arbitrary targets. WAL consumer workers
read the topic and perform delivery. Each target has an independent consumer group
so every configured target receives every event.

```text
wal.orders
  +-- group wal.orders.inventory --> inventory target
  +-- group wal.orders.billing   --> billing target
  +-- group wal.orders.analytics --> analytics target
```

Target definitions and retry policies live in PostgreSQL. The control-plane
database is not queried for every message; workers use their in-memory
configuration snapshot.

### Durable retries and dead letters

Per-message retry state is stored durably in Kafka rather than PostgreSQL:

```text
wal.orders
  +-- delivery succeeds --> commit offset
  +-- delivery fails    --> wal.orders.retry.<delay>
  +-- attempts exhausted --> wal.orders.dlq
```

A retry record contains the original event, target ID, attempt number, next
eligible time, and last error. The maximum attempts and backoff rules come from the
cached control-plane configuration. Retry records store the attempt number rather
than attempts remaining so policy changes do not rewrite existing records.

Publishing a retry or DLQ record and committing the consumed offset must happen in
one Kafka transaction. This prevents an acknowledged source record from losing its
retry state. Delay-tier topics will be used initially to avoid blocking a partition
while waiting for an individual record's retry time.

### Delivery milestones

1. Define PostgreSQL tables for namespaces, topics, targets, versions, and
   reconciliation status.
2. Implement control-plane CRUD APIs and Kafka topic reconciliation.
3. Implement versioned producer and consumer runtime-configuration endpoints.
4. Implement validated producer envelopes and durable Kafka publishing.
5. Implement per-target consumer groups and HTTP target delivery.
6. Add transactional retry topics, backoff tiers, and DLQ handling.
7. Add integration tests for provisioning, delivery, retry, restart recovery, and
   replay.
8. Add health checks, reconciliation metrics, delivery metrics, consumer lag, and
   configuration-staleness monitoring.
