"""Exercise 6: Manual Offset Commit

WHERE IT LIVES:
  consumer.py:88    — consumer.commit(message=msg) — only after successful write
  consumer.py:97-99 — OSError handler skips commit → message will be redelivered
  config.yml:7      — enable_auto_commit: false

THE CONCEPT:
  Kafka tracks "how far has this consumer group read" via committed offsets.
  With manual commits, YOU decide when to mark a message as processed.

  Your project commits only after a successful file write. If the write fails
  (OSError), the offset is NOT committed, so the next consumer in this group
  will re-read that message. This is AT-LEAST-ONCE delivery.

  Key insight: committing offset N means "I've processed everything up to
  and including offset N." It's not per-message — it's a high-water mark.
"""

import json
import uuid
import pytest
from confluent_kafka import Producer, Consumer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestManualOffsetCommit:

    def test_committed_message_not_redelivered(self, exercise_topic):
        """Baseline: commit → close → reopen = message is gone."""
        topic = exercise_topic(num_partitions=1)
        group = f"commit-test-{uuid.uuid4().hex[:8]}"

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=json.dumps({"title": "Committed"}).encode())
        p.flush()

        # Consumer 1: read and commit
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic])
        msg = c1.poll(timeout=10.0)
        assert msg is not None
        c1.commit(message=msg)
        c1.close()

        # Consumer 2: same group — message already consumed
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        msg2 = c2.poll(timeout=5.0)
        c2.close()

        # No message — offset was committed, so this consumer starts after it
        assert msg2 is None

    def test_uncommitted_message_redelivered(self, exercise_topic):
        """Skip commit → close → reopen = message comes back.

        PREDICT: If consumer reads but doesn't commit, what happens on restart?
        This is the core of at-least-once delivery.
        """
        topic = exercise_topic(num_partitions=1)
        group = f"nocommit-test-{uuid.uuid4().hex[:8]}"

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=json.dumps({"title": "Not Committed"}).encode())
        p.flush()

        # Consumer 1: read but do NOT commit
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic])
        msg = c1.poll(timeout=10.0)
        assert msg is not None
        # Deliberately skip: c1.commit(message=msg)
        c1.close()

        # Consumer 2: same group — message is redelivered
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        msg2 = c2.poll(timeout=10.0)
        c2.close()

        assert msg2 is not None
        value = json.loads(msg2.value().decode("utf-8"))
        assert value["title"] == "Not Committed"

    def test_commit_is_high_water_mark(self, exercise_topic):
        """Committing message 3 also marks messages 1 and 2 as done.

        PREDICT: If you consume 3 messages but only commit the 3rd,
        do messages 1 and 2 get redelivered?
        """
        topic = exercise_topic(num_partitions=1)
        group = f"watermark-test-{uuid.uuid4().hex[:8]}"

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(3):
            p.produce(topic, key=b"test", value=json.dumps({"seq": i}).encode())
        p.flush()

        # Read all 3, commit only the last one
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic])
        msgs = []
        for _ in range(3):
            msg = c1.poll(timeout=10.0)
            if msg:
                msgs.append(msg)
        assert len(msgs) == 3

        # Only commit the 3rd message
        c1.commit(message=msgs[2])
        c1.close()

        # New consumer in same group — all 3 are behind the committed offset
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        msg = c2.poll(timeout=5.0)
        c2.close()

        # No messages — committing offset 2 (the 3rd message, 0-indexed)
        # means "everything up to and including offset 2 is done"
        assert msg is None
