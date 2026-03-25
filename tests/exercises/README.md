# Kafka Concept Exercises

Interactive exercises tied to the Knowledge Stream Hub pipeline code.
Each exercise demonstrates a core Kafka concept with runnable tests.

## Prerequisites

- Docker running (`docker compose up -d` for Kafka broker)
- Python venv activated (`. .venv/bin/activate`)
- pytest installed (`pip install pytest`)

## How to run

```bash
# All exercises
./test.sh --exercises

# Single exercise
pytest tests/exercises/01_acks_levels.py -v --integration

# Specific test within an exercise
pytest tests/exercises/07_consumer_groups.py::TestConsumerGroups::test_two_consumers_share_partitions -v --integration
```

## Exercise checklist

### Producer
- [ ] 01 — Acks levels (acks=0/1/all)
- [ ] 02 — Delivery callbacks (async produce, flush behavior)
- [ ] 03 — Key-based partitioning (same key → same partition)
- [ ] 04 — Serialization (JSON round-trip, error handling)

### Consumer
- [ ] 05 — Poll loop (timeouts, blocking behavior)
- [ ] 06 — Manual offset commit (commit semantics, high-water mark)
- [ ] 07 — Consumer groups (partition sharing, independent groups)
- [ ] 08 — Rebalancing (on_assign/on_revoke callbacks)

### Broker
- [ ] 09 — Partitions and ordering (per-key vs total order)
- [ ] 10 — Retention (time-based expiry, cleanup)

### Operational
- [ ] 11 — Consumer lag (watermarks, committed offsets)
- [ ] 12 — At-least-once delivery (redelivery, idempotence, auto-commit risk)

## How each exercise works

1. Read the docstring — it explains the concept and points to where it lives in the project code
2. Run the baseline test — it passes and shows current behavior
3. Read the `# PREDICT:` comments — reason about what will happen before running
4. Run the experiment tests to verify your predictions
