"""Exercise 1: Producer Acknowledgments (acks)

WHERE IT LIVES:
  lib/kafka_helpers.py:21  — acks passed to Producer config
  config.yml:5             — acks: "all"

THE CONCEPT:
  The acks setting controls how many broker replicas must confirm receipt
  before the producer considers a message "delivered":

  - acks=0  → fire-and-forget, no confirmation at all
  - acks=1  → leader replica confirms (fast, but data lost if leader crashes)
  - acks=all → all in-sync replicas confirm (safest, highest latency)

  Your project uses acks=all because losing a note silently is worse than
  a few extra milliseconds of latency.
"""

import json
import threading
import pytest
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestAcksLevels:

    def test_acks_all_returns_valid_offset(self, exercise_topic, make_consumer):
        """Baseline: acks=all — delivery callback gets a real offset."""
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        results = []

        def on_delivery(err, msg):
            results.append({"err": err, "offset": msg.offset(), "partition": msg.partition()})

        p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
        p.produce(topic, key=b"test", value=b'{"title": "acks=all"}', callback=on_delivery)
        p.flush()

        assert len(results) == 1
        assert results[0]["err"] is None
        # With acks=all, we get a real offset back
        assert results[0]["offset"] >= 0

    def test_acks_zero_offset_is_unknown(self, exercise_topic):
        """acks=0: broker never confirms, so offset is unknown in the callback.

        PREDICT: What offset will the delivery callback report?
        """
        topic = exercise_topic(num_partitions=1)

        results = []

        def on_delivery(err, msg):
            results.append({"err": err, "offset": msg.offset()})

        p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "0"})
        p.produce(topic, key=b"test", value=b'{"title": "acks=0"}', callback=on_delivery)
        p.flush()

        assert len(results) == 1
        assert results[0]["err"] is None
        # acks=0: the producer doesn't wait for acknowledgment
        # The message was sent but we don't know if it arrived
        # Offset is None — the broker never sent an acknowledgment
        assert results[0]["offset"] is None

    def test_acks_one_returns_valid_offset(self, exercise_topic):
        """acks=1: leader confirms, offset is valid.

        PREDICT: How will this differ from acks=all on a single-broker setup?
        """
        topic = exercise_topic(num_partitions=1)

        results = []

        def on_delivery(err, msg):
            results.append({"err": err, "offset": msg.offset()})

        p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "1"})
        p.produce(topic, key=b"test", value=b'{"title": "acks=1"}', callback=on_delivery)
        p.flush()

        assert len(results) == 1
        assert results[0]["err"] is None
        # With a single broker, acks=1 and acks=all behave identically
        # because the leader IS the only replica. The difference matters
        # in multi-broker setups where acks=all waits for followers too.
        assert results[0]["offset"] >= 0
