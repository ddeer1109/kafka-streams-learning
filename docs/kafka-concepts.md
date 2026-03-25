# Kafka Concepts — Knowledge Stream Hub

Reference mapping each Kafka concept to where it lives in the project code.
Cross-reference with `tests/exercises/` for hands-on practice.

---

## Producer

### Acknowledgments (acks)
- **Where**: `lib/kafka_helpers.py:21`, `config.yml:5`
- **Current setting**: `acks=all` — waits for all in-sync replicas to confirm
- **What happens when you change it**:
  - `acks=0` — fire-and-forget, no confirmation, offset unknown in callback
  - `acks=1` — leader-only confirmation, faster but risks data loss on leader crash
  - `acks=all` — safest, highest latency (on single broker, same as acks=1)
- **Why "all" for this project**: losing a note silently is worse than a few extra ms
- **Exercise**: `tests/exercises/01_acks_levels.py`

### Delivery Callbacks
- **Where**: `lib/kafka_helpers.py:8-15` (callback), `lib/kafka_helpers.py:32` (flush)
- **What it does**: fires after broker acknowledges (or rejects) each message
- **Key detail**: `produce()` is async — callbacks fire during `flush()` or `poll()`, not during `produce()`
- **Your project**: calls `flush()` after every message (synchronous pattern)
- **Exercise**: `tests/exercises/02_delivery_callbacks.py`

### Key-Based Partitioning
- **Where**: `producer.py:80` (key=category), `lib/kafka_helpers.py:27` (key encoding)
- **Current key**: category string (e.g., "kafka", "devops")
- **Effect**: all notes in the same category go to the same partition → ordered within category
- **Trade-off**: if one category dominates, its partition gets hot
- **Null key**: round-robin distribution, no ordering guarantee
- **Exercise**: `tests/exercises/03_key_partitioning.py`

### Serialization
- **Where**: `lib/kafka_helpers.py:29` (producer: json.dumps + UTF-8), `consumer.py:65` (consumer: decode + json.loads)
- **Format**: JSON — simple, human-readable, no schema enforcement
- **Error handling**: `consumer.py:67-69` catches JSONDecodeError and skips bad messages
- **Production alternative**: Avro/Protobuf with Schema Registry (not needed here)
- **Exercise**: `tests/exercises/04_serialization.py`

---

## Consumer

### Poll Loop
- **Where**: `consumer.py:56-61`
- **Timeout**: `1.0` second — returns `None` when no messages available
- **Why 1.0**: fast enough to respond to Ctrl+C (checked via `running` flag), slow enough to avoid CPU spinning
- **Internal work**: poll() also sends heartbeats, triggers rebalance callbacks, and handles auto-commit
- **Exercise**: `tests/exercises/05_poll_loop.py`

### Manual Offset Commit
- **Where**: `consumer.py:88` (success path), `consumer.py:97-99` (failure path)
- **Config**: `enable_auto_commit: false` in `config.yml:7`
- **Pattern**: commit AFTER side effect (file write) succeeds
- **Key insight**: committing offset N means "everything up to and including N is done" — it's a high-water mark, not per-message
- **Exercise**: `tests/exercises/06_manual_offset_commit.py`

### Consumer Groups
- **Where**: `config.yml:4` (group name), `lib/kafka_helpers.py:38` (group.id config)
- **Current group**: `notes-to-pcloud`
- **How it works**: partitions are distributed among consumers in the same group
- **Scaling limit**: with 3 partitions, max 3 useful consumers (4th would idle)
- **Independent groups**: consumers in different groups get independent copies of all messages
- **Exercise**: `tests/exercises/07_consumer_groups.py`

### Rebalancing
- **Where**: `consumer.py:50` — `subscribe()` currently has NO rebalance callback
- **What's missing**: `on_assign` / `on_revoke` callbacks
- **When it happens**: consumer joins/leaves group, topic partition count changes
- **Why it matters**: during rebalance, consumption pauses; `on_revoke` is the place to flush state and commit offsets
- **Exercise**: `tests/exercises/08_rebalancing.py`

---

## Broker

### Topics and Partitions
- **Where**: `run.sh:25-30` (topic creation), `docker-compose.yml:35` (auto-create enabled)
- **Current**: `notes.inbound` with 3 partitions, replication factor 1
- **Ordering**: guaranteed within a partition, not across partitions
- **Trade-off**: more partitions = more parallelism, but no cross-partition ordering
- **Exercise**: `tests/exercises/09_partitions_and_ordering.py`

### Retention
- **Where**: not explicitly configured — broker default is 7 days (`retention.ms=604800000`)
- **What it means**: old notes stay in Kafka for a week, then are deleted automatically
- **Config options**: `retention.ms` (time), `retention.bytes` (size), `cleanup.policy` (delete vs compact)
- **For this project**: pCloud files are the permanent record; Kafka is the transport
- **Exercise**: `tests/exercises/10_retention.py`

---

## Operational

### Consumer Lag
- **What it is**: `high_watermark - committed_offset` per partition
- **How to see it**: Kafka UI at `localhost:8090` → Consumer Groups → `notes-to-pcloud`
- **Programmatic**: `consumer.get_watermark_offsets()` + `consumer.committed()`
- **What it tells you**: lag=0 means caught up; growing lag means consumer is falling behind
- **Exercise**: `tests/exercises/11_consumer_lag.py`

### At-Least-Once vs Exactly-Once
- **Where**: `consumer.py:80-99` (commit-after-write pattern)
- **Current guarantee**: at-least-once (crash before commit = redelivery, possible duplicate)
- **At-most-once**: commit first, then process — if processing fails, message is lost (this is what `enable.auto.commit: true` effectively gives you)
- **Exactly-once**: requires Kafka transactions + idempotent producer — overkill for this project
- **Practical mitigation**: make processing idempotent (check before appending)
- **Exercise**: `tests/exercises/12_at_least_once.py`
