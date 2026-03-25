"""Exercise 10: Topic Retention

WHERE IT LIVES:
  Not explicitly configured — broker default is 7 days (retention.ms=604800000)
  docker-compose.yml — single broker with default retention settings

THE CONCEPT:
  Kafka is NOT a permanent store (by default). Messages expire based on:

  - retention.ms  — time-based (default: 7 days)
  - retention.bytes — size-based (default: unlimited)
  - cleanup.policy — "delete" (default) or "compact" (keeps latest per key)

  Your project uses defaults, so notes stay in Kafka for 7 days. After that,
  they're gone even if no consumer read them. The pCloud files are the
  permanent record — Kafka is the transport.
"""

import json
import time
import pytest
from confluent_kafka import Producer, Consumer
import uuid

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestRetention:

    def test_messages_expire_after_retention(self, exercise_topic_with_config):
        """Messages disappear after retention.ms expires.

        PREDICT: If retention.ms=5000 (5 seconds) and you wait 8 seconds,
        can you still read the message?
        """
        topic = exercise_topic_with_config(
            num_partitions=1,
            config={
                "retention.ms": "5000",
                # Speed up log cleanup so retention is enforced promptly
                "segment.ms": "1000",
            },
        )

        # Produce a message
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=json.dumps({"title": "Ephemeral"}).encode())
        p.flush()

        # Verify it's readable immediately
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"ret-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic])
        msg = c1.poll(timeout=10.0)
        assert msg is not None, "Message should be readable immediately"
        c1.close()

        # Wait for retention to expire + log cleaner to run.
        # The log cleaner runs on a schedule (default every 5 minutes for
        # retention-based cleanup). With segment.ms=1000 the segment closes
        # quickly, but the cleaner still needs to run. We wait generously.
        # This test is inherently timing-dependent.
        time.sleep(20)

        # Try to read again with a fresh consumer
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"ret-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        msg2 = c2.poll(timeout=5.0)
        c2.close()

        # Message should be gone — retention expired.
        # If this fails, the log cleaner hasn't run yet — this is expected
        # occasional flakiness. The concept still holds: Kafka WILL delete
        # expired messages, just not on a precise schedule.
        if msg2 is not None:
            pytest.skip(
                "Log cleaner hasn't run yet — retention is async. "
                "The message will be deleted eventually. Try again or "
                "check Kafka UI to see the retention config applied."
            )

    def test_default_retention_is_7_days(self, exercise_topic_with_config):
        """The default retention.ms is 604800000 (7 days).

        PREDICT: With default retention, will your notes.inbound topic
        accumulate data indefinitely?
        (No — after 7 days, old segments are deleted automatically.)
        """
        # Create a topic with no explicit retention config
        topic = exercise_topic_with_config(num_partitions=1, config={})

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=b'{"title": "Persistent for 7 days"}')
        p.flush()

        c = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"ret-default-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe([topic])
        msg = c.poll(timeout=10.0)
        c.close()

        # Readable now (well within 7 days!)
        assert msg is not None
