"""Exercise 5: The Consumer Poll Loop

WHERE IT LIVES:
  consumer.py:57  — msg = consumer.poll(timeout=1.0)
  consumer.py:58  — if msg is None: continue

THE CONCEPT:
  The poll loop is the heart of a Kafka consumer. poll(timeout) blocks for
  up to `timeout` seconds waiting for a message. It returns:
  - A message object if one is available
  - None if the timeout expires with no messages

  The timeout controls the trade-off between responsiveness (how fast you
  react to new messages) and CPU usage (how often you spin idle).

  poll() also handles internal housekeeping: heartbeats to the group
  coordinator, rebalance callbacks, and offset commits (if auto-commit
  is enabled).
"""

import json
import time
import threading
import pytest
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestPollLoop:

    def test_poll_returns_none_when_empty(self, exercise_topic, make_consumer):
        """Baseline: poll() returns None when no messages are available."""
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        # No messages produced — poll should return None
        start = time.time()
        msg = consumer.poll(timeout=1.0)
        elapsed = time.time() - start

        assert msg is None
        # It should have waited approximately 1 second
        assert 0.8 < elapsed < 3.0

    def test_poll_returns_immediately_with_message(self, exercise_topic, make_consumer):
        """When a message is ready, poll() returns it without waiting.

        PREDICT: If timeout=10.0 but a message is already in the topic,
        how long does poll() take?
        """
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        # Produce a message first
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=b'{"title": "ready"}')
        p.flush()

        # Even with a long timeout, poll returns as soon as a message is available
        start = time.time()
        msg = consumer.poll(timeout=10.0)
        elapsed = time.time() - start

        assert msg is not None
        assert elapsed < 5.0  # should be much faster than 10s

    def test_zero_timeout_nonblocking(self, exercise_topic, make_consumer):
        """timeout=0 makes poll() non-blocking — returns immediately.

        PREDICT: Is timeout=0 ever useful in practice?
        (Yes — when you need to drain the internal queue without blocking,
        e.g., during shutdown.)
        """
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        start = time.time()
        msg = consumer.poll(timeout=0.0)
        elapsed = time.time() - start

        assert msg is None
        assert elapsed < 0.5  # should be near-instant

    def test_poll_receives_message_produced_during_wait(self, exercise_topic, make_consumer):
        """Producer sends while consumer is polling — consumer wakes up.

        PREDICT: Consumer starts polling with timeout=10s. Producer sends
        after 2s. Does the consumer wait the full 10s?
        """
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        def delayed_produce():
            time.sleep(2)
            p = Producer({"bootstrap.servers": BOOTSTRAP})
            p.produce(topic, key=b"delayed", value=b'{"title": "late arrival"}')
            p.flush()

        t = threading.Thread(target=delayed_produce)
        t.start()

        start = time.time()
        msg = consumer.poll(timeout=10.0)
        elapsed = time.time() - start

        t.join()

        assert msg is not None
        # Consumer should wake up around 2s, not wait the full 10s
        assert elapsed < 8.0
        decoded = json.loads(msg.value().decode("utf-8"))
        assert decoded["title"] == "late arrival"
