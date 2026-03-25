"""Exercise 11: Consumer Lag

WHERE IT LIVES:
  Not implemented in the project code — this is an operational concept.
  Visible in Kafka UI at localhost:8090 → Consumer Groups → notes-to-pcloud

THE CONCEPT:
  Consumer lag = how far behind a consumer is from the latest available message.

  Formally: lag = high_watermark - committed_offset

  - high_watermark: the offset of the NEXT message the broker will assign
  - committed_offset: the last offset this consumer group committed

  If lag is 0 → consumer is caught up.
  If lag is growing → consumer is falling behind (processing too slow,
  consumer crashed, etc.)

  This is the #1 operational metric for Kafka consumers. Your Kafka UI
  at localhost:8090 shows this per-partition for each consumer group.
"""

import json
import uuid
import pytest
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestConsumerLag:

    def test_compute_lag_programmatically(self, exercise_topic):
        """Produce messages, then compute lag before consuming.

        PREDICT: After producing 10 messages with no consumer, what is the lag?
        """
        topic = exercise_topic(num_partitions=1)
        group = f"lag-{uuid.uuid4().hex[:8]}"

        # Produce 10 messages
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(10):
            p.produce(topic, key=b"test", value=json.dumps({"seq": i}).encode())
        p.flush()

        # Use a consumer to get watermark offsets
        c = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe([topic])
        # Wait for assignment
        for _ in range(30):
            c.poll(timeout=0.5)
            if c.assignment():
                break

        tp = TopicPartition(topic, 0)

        # Get the high and low watermarks
        low, high = c.get_watermark_offsets(tp)

        # high watermark = offset of NEXT message to be written = 10
        assert high == 10
        # low watermark = earliest available offset = 0
        assert low == 0

        # Committed offset: none yet (consumer hasn't committed)
        committed = c.committed([tp])
        committed_offset = committed[0].offset
        # -1001 means "no committed offset" in confluent-kafka
        assert committed_offset == -1001

        # Lag = high_watermark - committed_offset
        # Since no offset is committed, effective lag is all 10 messages
        effective_lag = high - 0  # treating "no offset" as 0 with auto.offset.reset=earliest
        assert effective_lag == 10

        c.close()

    def test_lag_decreases_as_consumer_reads(self, exercise_topic):
        """Consume messages and watch lag decrease.

        PREDICT: After consuming half the messages, what is the lag?
        """
        topic = exercise_topic(num_partitions=1)
        group = f"lag-{uuid.uuid4().hex[:8]}"

        # Produce 10 messages
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(10):
            p.produce(topic, key=b"test", value=json.dumps({"seq": i}).encode())
        p.flush()

        c = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe([topic])
        for _ in range(30):
            c.poll(timeout=0.5)
            if c.assignment():
                break

        tp = TopicPartition(topic, 0)
        _, high = c.get_watermark_offsets(tp)
        assert high == 10

        # Consume all available messages (the assignment poll may have
        # already consumed some, so we drain rather than expect exactly 10)
        messages = []
        while True:
            msg = c.poll(timeout=3.0)
            if msg is None:
                break
            if not msg.error():
                messages.append(msg)
        assert len(messages) >= 5, f"Expected at least 5 messages, got {len(messages)}"

        # Commit a message in the middle
        mid = len(messages) // 2
        c.commit(message=messages[mid])
        committed = c.committed([tp])
        mid_lag = high - committed[0].offset
        assert mid_lag > 0, f"Expected positive lag at midpoint, got {mid_lag}"

        # Commit the last message — lag should be 0
        c.commit(message=messages[-1])
        committed = c.committed([tp])
        final_lag = high - committed[0].offset
        assert final_lag == 0, f"Expected lag=0, got {final_lag}"

        c.close()
