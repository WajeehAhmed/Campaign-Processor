
# Campaign Processor — Phase 2: Distributed Streaming Worker & Aggregator

This module is the core real-time processing engine of the Campaign Analytics system. Built with Spring Boot and Spring Kafka, it horizontally scales across a multi-worker consumer group to ingest raw streaming events, enforce strict data idempotency, and aggregate metrics on-the-fly directly into the relational read-path storage.

## Architectural Lifecycle


```
         [ Kafka Topic: campaign-events-raw ]
                          │
           (Distributed Partition Assignment)
                          │
                          ▼
             ┌─────────────────────────┐
             │  EventConsumerService   │
             └────────────┬────────────┘
                          │
             1. Extract Kafka Record Key
                          │
                          ▼
             ┌─────────────────────────┐
             │  Redis Idempotency Gate │  
                (Check Key: 
                `event:processed:{id}`)
             └────────────┬────────────┘
                          │
                 [ Is Unique Event? ]
                  ├── No  ──► [ Drop Duplicate & Commit ]
                  └── Yes ──► [ Proceed to Data Pipeline ]
                                   │
                                   ▼
             ┌─────────────────────────┐
             │  Time Window Truncation │  
             (Compute ISO-8601 Hour Bucket)
             └────────────┬────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼

┌───────────────────────┐       ┌───────────────────────┐
│   Write-Path Logger   │       │ Read-Path Aggregator  │
├───────────────────────┤       ├───────────────────────┤
│ Save Raw Auditing Log │       │ Atomic DB UPSERT      │
│ (`raw_events` Table)  │       │ (`campaign_stats`)    │
└───────────────────────┘       └───────────────────────┘

```

---

## Technical Architecture & Core Pillars

### 1. Key-Based Order Guarantee
By leveraging Kafka's classic consumer group protocol, individual partitions are assigned exclusively to distinct worker threads. Because payloads are dispatched from the gateway using the `campaignId` as the partition routing key, all sequential history for a given campaign is bound to a single worker, eliminating distributed database locking conditions.

### 2. Atomic Cache Filters (Idempotency)
To neutralize the risk of duplicate writes from network retries, the worker implements an inline processing block using a Redis distributed cache cluster:
* Executes an atomic `SETNX` verification with a strict 24-hour Time-To-Live (TTL).
* If the unique event ID token already exists, the record is flagged, skipped, and acknowledged immediately without hitting downstream resources.

### 3. Inline Streaming Aggregation
Rather than computing batch jobs at intervals, the worker evaluates metrics instantaneously upon packet arrival. It truncates event timestamps to the nearest chronological hour bucket and triggers an atomic native SQL `UPSERT` statement against the presentation layer, drastically shrinking metric latency to sub-millisecond intervals.

### 4. Non-Blocking Fault Isolation (DLQ Layer)
Engineered for zero-downtime tolerance, standard validation issues or poison pill structures cannot halt global execution:
* **Local Retry Layer:** A customized `DefaultErrorHandler` intercepts runtime processing drops and schedules up to 3 local delivery attempts (initial + 2 retries) bounded by a 2-second `FixedBackOff` window.
* **Dead-Letter Routing:** If local retries are exhausted, the record bypasses active worker context and is safely marshaled to the isolated `campaign-events-raw-dlt` channel, allowing the offset pointer to commit and advance smoothly.

---

## Technical Specifications

* **Runtime:** Java 21 (Long-Term Support)
* **Framework:** Spring Boot v4.0.6 & Spring Kafka v4.0.5
* **Relational Layer:** Hibernate ORM 7.2 / PostgreSQL Driver (Native Dialect)
* **Caching Integration:** Spring Data Redis (`StringRedisTemplate`)

---

## Local Development Execution

### Core Database Configuration (`application.properties`)
Ensure the application runtime points to your local infrastructure coordinates inside the class resources directory:

```properties
spring.application.name=campaign-processor
server.port=8090

# Persistence Coordinates
spring.datasource.url=jdbc:postgresql://localhost:5432/analyzer
spring.datasource.username=postgres
spring.datasource.password=postgres

# Distributed Layer Configuration
spring.kafka.bootstrap-servers=localhost:29092
spring.kafka.consumer.group-id=campaign-processor-group
spring.kafka.consumer.auto-offset-reset=earliest

# Cache Configuration
spring.data.redis.host=localhost
spring.data.redis.port=6379

# Logging Levels
logging.level.com.cipherlab.campaign_processor=INFO

```

### Compiling and Initializing Worker Cluster

Launch multiple parallel terminals to observe horizontal load balancing across different ports:

```bash
# Terminal 1 - Spin up Instance A
./mvnw clean compile
./mvnw spring-boot:run -Dspring-boot.run.arguments="--server.port=8090"

# Terminal 2 - Spin up Instance B (Horizontally scaled)
./mvnw spring-boot:run -Dspring-boot.run.arguments="--server.port=8091"

```

---

## Operational Verification

To inspect active partition allocations and confirm how your running instances divide the message ingestion lanes, run the native Kafka diagnostic utility inside your local Docker cluster environment:

```bash
docker exec -it caampaign-analytics-kafka-1 \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group campaign-processor-group

```