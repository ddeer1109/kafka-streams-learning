"""Exercise 9: Partition Ordering Guarantees

WHERE IT LIVES:
  run.sh:25-30  — kafka-topics --create --partitions 3
  producer.py:80 — key=category determines which partition

THE CONCEPT:
  Kafka guarantees message ordering WITHIN a partition, not across partitions.

  - Same key → same partition → messages are ordered
  - Different keys → may hit different partitions → no cross-key ordering
  - 1 partition → total ordering (but no parallelism)
  - N partitions → parallel consumption, per-key ordering only

  Your project uses 3 partitions with category as the key. This means:
  all "kafka" notes are ordered, all "devops" notes are ordered, but
  a "kafka" note and a "devops" note have no ordering guarantee relative
  to each other.
"""

import json
import pytest
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestPartitionsAndOrdering:

    def test_same_key_preserves_order(self, exercise_topic, make_consumer):
        """Baseline: messages with the same key arrive in order."""
        topic = exercise_topic(num_partitions=3)
        consumer = make_consumer(topic)

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(10):
            p.produce(topic, key=b"kafka", value=json.dumps({"seq": i}).encode())
        p.flush()

        received = []
        for _ in range(10):
            msg = consumer.poll(timeout=10.0)
            if msg and not msg.error():
                received.append(json.loads(msg.value())["seq"])

        assert received == list(range(10)), f"Messages out of order: {received}"

    def test_different_keys_per_key_ordering(self, exercise_topic, make_consumer):
        """Different keys: per-key order preserved, but cross-key order is NOT guaranteed.

        PREDICT: If you interleave produce("A",1), produce("B",1), produce("A",2),
        produce("B",2) — will the consumer see A1,B1,A2,B2 in that exact order?
        """
        topic = exercise_topic(num_partitions=3)
        consumer = make_consumer(topic)

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        # Interleave two keys
        for i in range(5):
            p.produce(topic, key=b"alpha", value=json.dumps({"key": "alpha", "seq": i}).encode())
            p.produce(topic, key=b"beta", value=json.dumps({"key": "beta", "seq": i}).encode())
        p.flush()

        # Collect per-key sequences
        alpha_seqs = []
        beta_seqs = []
        for _ in range(10):
            msg = consumer.poll(timeout=10.0)
            if msg and not msg.error():
                value = json.loads(msg.value())
                if value["key"] == "alpha":
                    alpha_seqs.append(value["seq"])
                else:
                    beta_seqs.append(value["seq"])

        # Per-key ordering is guaranteed
        assert alpha_seqs == list(range(5)), f"Alpha out of order: {alpha_seqs}"
        assert beta_seqs == list(range(5)), f"Beta out of order: {beta_seqs}"

        # But the overall interleaving may differ from produce order
        # (alpha and beta might be on different partitions, consumed sequentially)

    def test_single_partition_total_order(self, exercise_topic, make_consumer):
        """1 partition = total ordering regardless of keys. Trade-off: no parallelism.

        PREDICT: With only 1 partition, can you have 2 active consumers?
        """
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        keys = [b"kafka", b"devops", b"lyrics", b"kafka", b"devops"]
        for i, key in enumerate(keys):
            p.produce(topic, key=key, value=json.dumps({"seq": i, "key": key.decode()}).encode())
        p.flush()

        received = []
        for _ in range(5):
            msg = consumer.poll(timeout=10.0)
            if msg and not msg.error():
                received.append(json.loads(msg.value())["seq"])

        # With 1 partition, total ordering is preserved
        assert received == [0, 1, 2, 3, 4], f"Expected total order, got: {received}"
