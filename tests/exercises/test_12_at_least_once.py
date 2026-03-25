"""Exercise 12: At-Least-Once Delivery

WHERE IT LIVES:
  consumer.py:80-99  — commit-after-write pattern
  consumer.py:88     — consumer.commit(message=msg) after successful append_note()
  consumer.py:97-99  — OSError: don't commit → message will be redelivered
  config.yml:7       — enable_auto_commit: false

THE CONCEPT:
  Your project implements AT-LEAST-ONCE delivery:
  1. Read message from Kafka
  2. Write to markdown file
  3. Commit offset

  If step 2 fails, step 3 never happens, so the message is redelivered.
  This means a message might be processed TWICE (if step 2 succeeds but
  the consumer crashes before step 3).

  The alternatives:
  - AT-MOST-ONCE: commit first, then process. If processing fails, message is lost.
  - EXACTLY-ONCE: requires Kafka transactions — overkill for this project.

  Practical mitigation for duplicates: make your processing IDEMPOTENT
  (check if the note already exists before appending).
"""

import json
import os
import uuid
import pytest
from confluent_kafka import Producer, Consumer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestAtLeastOnce:

    def test_failure_before_commit_causes_redelivery(self, exercise_topic, tmp_path):
        """Simulate a write failure — message gets redelivered.

        This is exactly what happens in consumer.py:97-99.
        """
        topic = exercise_topic(num_partitions=1)
        group = f"alo-{uuid.uuid4().hex[:8]}"

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=json.dumps({"title": "Important Note"}).encode())
        p.flush()

        # Consumer 1: read the message, "fail" the write, don't commit
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic])
        msg1 = c1.poll(timeout=10.0)
        assert msg1 is not None
        # Simulate write failure — skip commit
        c1.close()

        # Consumer 2: same group, message is redelivered
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        msg2 = c2.poll(timeout=10.0)
        assert msg2 is not None

        value = json.loads(msg2.value().decode("utf-8"))
        assert value["title"] == "Important Note"

        # This time, "write" succeeds — commit
        c2.commit(message=msg2)
        c2.close()

    def test_idempotent_processing_prevents_duplicates(self, exercise_topic, tmp_path):
        """Make processing idempotent — safe even with redelivery.

        PREDICT: If the same message is processed twice, does the file
        have the note once or twice?
        """
        topic = exercise_topic(num_partitions=1)
        filepath = str(tmp_path / "notes.md")

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        msg_data = {"title": "Unique Note", "id": "note-123"}
        p.produce(topic, key=b"test", value=json.dumps(msg_data).encode())
        p.flush()

        def idempotent_write(filepath, title, note_id):
            """Only append if this note_id hasn't been written yet."""
            if os.path.exists(filepath):
                existing = open(filepath).read()
                if f"[{note_id}]" in existing:
                    return  # already written — skip
            with open(filepath, "a") as f:
                f.write(f"\n## {title}\n[{note_id}]\n---\n")

        # Process the same message twice (simulating redelivery)
        group = f"idemp-{uuid.uuid4().hex[:8]}"
        for attempt in range(2):
            c = Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            c.subscribe([topic])
            msg = c.poll(timeout=10.0)
            if msg and not msg.error():
                value = json.loads(msg.value().decode("utf-8"))
                idempotent_write(filepath, value["title"], value["id"])
            # Don't commit on first attempt (simulate crash)
            if attempt == 1:
                c.commit(message=msg)
            c.close()

        # Despite two deliveries, the note appears only once
        text = open(filepath).read()
        assert text.count("Unique Note") == 1

    def test_auto_commit_loses_messages(self, exercise_topic, tmp_path):
        """With auto-commit, a failed write means the message is LOST.

        PREDICT: enable.auto.commit=true — if the write fails but the
        consumer keeps polling, is the offset committed anyway?
        """
        topic = exercise_topic(num_partitions=1)
        group = f"autocommit-{uuid.uuid4().hex[:8]}"

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=json.dumps({"title": "Lost Note"}).encode())
        p.flush()

        # Consumer with auto-commit enabled
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": "100",  # fast auto-commit
        })
        c1.subscribe([topic])
        msg = c1.poll(timeout=10.0)
        assert msg is not None

        # Simulate a write failure — but the consumer doesn't know
        # It just keeps polling, and auto-commit commits the offset
        # Give auto-commit time to fire
        for _ in range(5):
            c1.poll(timeout=0.5)
        c1.close()

        # New consumer in same group — message is gone
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        msg2 = c2.poll(timeout=5.0)
        c2.close()

        # The message was auto-committed even though we "failed" to process it
        # This is AT-MOST-ONCE delivery — and why config.yml disables auto-commit
        assert msg2 is None, "Message was lost due to auto-commit — this is at-most-once"
