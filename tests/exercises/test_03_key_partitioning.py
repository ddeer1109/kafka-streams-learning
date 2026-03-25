"""Exercise 3: Key-Based Partition Assignment

WHERE IT LIVES:
  producer.py:80           — produce_note(..., key=category, ...)
  lib/kafka_helpers.py:27  — key.encode("utf-8") as partition key

THE CONCEPT:
  Kafka uses a hash of the message key to determine which partition it goes to.
  Same key → same partition → ordered within that key.

  Your project uses the category string ("kafka", "devops", etc.) as the key,
  so all notes in the same category are ordered and go to the same partition.

  If key is None, Kafka uses round-robin (or sticky partitioning) across
  partitions — no ordering guarantee.
"""

import json
from collections import defaultdict
import pytest
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestKeyPartitioning:

    def test_same_key_same_partition(self, exercise_topic, make_consumer):
        """Baseline: same key always lands on the same partition."""
        topic = exercise_topic(num_partitions=3)
        consumer = make_consumer(topic)

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        partitions = set()

        results = []

        def on_delivery(err, msg):
            results.append(msg.partition())

        for i in range(10):
            p.produce(topic, key=b"kafka", value=f'{{"seq": {i}}}'.encode(), callback=on_delivery)
        p.flush()

        assert len(set(results)) == 1, f"Same key landed on multiple partitions: {set(results)}"

    def test_different_keys_distribute(self, exercise_topic):
        """Different keys may land on different partitions.

        PREDICT: With 3 partitions and 5 keys, will all partitions be used?
        """
        topic = exercise_topic(num_partitions=3)

        key_to_partition = {}

        def on_delivery(err, msg):
            key_to_partition[msg.key()] = msg.partition()

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        keys = [b"kafka", b"devops", b"lyrics", b"training", b"swiss"]
        for key in keys:
            p.produce(topic, key=key, value=b'{"data": 1}', callback=on_delivery)
        p.flush()

        # Each key consistently maps to one partition
        assert len(key_to_partition) == 5

        # With 5 keys and 3 partitions, at least 2 partitions should be used
        # (the exact distribution depends on the hash function)
        unique_partitions = set(key_to_partition.values())
        assert len(unique_partitions) >= 2, (
            f"Expected distribution across partitions, got: {key_to_partition}"
        )

    def test_null_key_distributes_across_partitions(self, exercise_topic):
        """No key → messages spread across partitions (round-robin / sticky).

        PREDICT: With key=None and 20 messages on 3 partitions, will messages
        land on more than one partition?
        """
        topic = exercise_topic(num_partitions=3)

        partition_counts = defaultdict(int)

        def on_delivery(err, msg):
            partition_counts[msg.partition()] += 1

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(20):
            p.produce(topic, value=f'{{"seq": {i}}}'.encode(), callback=on_delivery)
        p.flush()

        # Without a key, messages should eventually hit multiple partitions
        # (sticky partitioner may batch to one partition initially, but
        #  across 20 messages it typically spreads)
        assert len(partition_counts) >= 2, (
            f"Expected multi-partition distribution, got: {dict(partition_counts)}"
        )
