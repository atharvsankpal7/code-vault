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
