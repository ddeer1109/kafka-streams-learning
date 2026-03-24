"""Integration tests — require a running Kafka broker (docker compose up -d).

Uses isolated test topics and consumer groups so real pipeline data is never touched.
pCloud output goes to a temp directory, cleaned up automatically.
"""

import json
import os
import uuid
import pytest
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from lib.markdown import append_note
from lib.kafka_helpers import produce_note

BOOTSTRAP = "localhost:9092"

# Each test run gets unique topic/group names to avoid cross-run interference
RUN_ID = uuid.uuid4().hex[:8]
TEST_TOPIC = f"test.notes.{RUN_ID}"


def _unique_group():
    return f"test-group-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def kafka_admin():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    # Create test topic
    topic = NewTopic(TEST_TOPIC, num_partitions=3, replication_factor=1)
    fs = admin.create_topics([topic])
    for t, f in fs.items():
        f.result()  # block until created
    yield admin
    # Cleanup: delete test topic
    fs = admin.delete_topics([TEST_TOPIC])
    for t, f in fs.items():
        try:
            f.result()
        except Exception:
            pass


@pytest.fixture
def test_producer(kafka_admin):
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    yield p


@pytest.fixture
def test_consumer(kafka_admin):
    """Each test gets its own consumer group — no cross-test message leaking.

    Uses auto.offset.reset=latest so it only sees messages produced during the test.
    Waits for partition assignment before yielding to avoid race conditions.
    """
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": _unique_group(),
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    c.subscribe([TEST_TOPIC])
    # Wait until partitions are actually assigned before the test produces
    for _ in range(30):
        c.poll(timeout=0.5)
        if c.assignment():
            break
    else:
        raise RuntimeError("Consumer failed to get partition assignment within 15s")
    yield c
    c.close()


@pytest.fixture
def pcloud_dir(tmp_path):
    """Temp directory simulating /mnt/p/KnowledgeHub/. Cleaned up by pytest."""
    return tmp_path


# --- Tests ---


@pytest.mark.integration
class TestKafkaRoundTrip:

    def test_produce_consume_message(self, test_producer, test_consumer):
        """Produce a message and consume it back — basic Kafka round-trip."""
        msg = {"title": "Test Note", "category": "kafka", "tags": ["kafka"]}
        test_producer.produce(
            TEST_TOPIC,
            key=b"kafka",
            value=json.dumps(msg).encode("utf-8"),
        )
        test_producer.flush()

        received = test_consumer.poll(timeout=10.0)
        assert received is not None, "No message received within timeout"
        assert received.error() is None
        value = json.loads(received.value().decode("utf-8"))
        assert value["title"] == "Test Note"
        assert value["category"] == "kafka"

    def test_key_based_partitioning(self, test_producer, test_consumer):
        """Messages with the same key always land on the same partition."""
        partitions = set()
        for i in range(5):
            test_producer.produce(
                TEST_TOPIC,
                key=b"same-key",
                value=json.dumps({"seq": i}).encode("utf-8"),
            )
        test_producer.flush()

        for _ in range(5):
            msg = test_consumer.poll(timeout=10.0)
            if msg and not msg.error():
                partitions.add(msg.partition())

        assert len(partitions) == 1, f"Same key landed on multiple partitions: {partitions}"

    def test_produce_note_helper(self, test_consumer):
        """Test produce_note() serialization through a real broker."""
        producer = Producer({"bootstrap.servers": BOOTSTRAP})
        value = {"title": "Helper Test", "category": "devops", "tags": ["devops"]}
        produce_note(producer, TEST_TOPIC, "devops", value)

        msg = test_consumer.poll(timeout=10.0)
        assert msg is not None
        decoded = json.loads(msg.value().decode("utf-8"))
        assert decoded["title"] == "Helper Test"


@pytest.mark.integration
class TestPipelineEndToEnd:

    def test_produce_consume_write_markdown(self, test_producer, test_consumer, pcloud_dir):
        """Full pipeline: produce → consume → write to markdown file."""
        note = {
            "title": "E2E Test Note",
            "content": "This should end up in a file",
            "category": "kafka",
            "tags": ["kafka", "testing"],
            "timestamp": "2026-03-24 10:00",
        }
        test_producer.produce(
            TEST_TOPIC,
            key=b"kafka",
            value=json.dumps(note).encode("utf-8"),
        )
        test_producer.flush()

        msg = test_consumer.poll(timeout=10.0)
        assert msg is not None and msg.error() is None

        value = json.loads(msg.value().decode("utf-8"))

        # Route to temp pcloud dir
        filepath = str(pcloud_dir / f"{value['category']}-inbox.md")
        append_note(
            filepath,
            title=value["title"],
            content=value.get("content", ""),
            tags=value.get("tags"),
            timestamp=value.get("timestamp"),
        )
        test_consumer.commit(message=msg)

        # Verify file was written correctly
        text = open(filepath).read()
        assert "## E2E Test Note" in text
        assert "This should end up in a file" in text
        assert "Tags: #kafka, #testing" in text
        assert "*2026-03-24 10:00*" in text
        assert "---" in text

    def test_routing_to_different_files(self, test_producer, test_consumer, pcloud_dir):
        """Messages with different categories route to different files."""
        routing = {
            "kafka": str(pcloud_dir / "kafka-inbox.md"),
            "devops": str(pcloud_dir / "devops-inbox.md"),
            "_default": str(pcloud_dir / "general-inbox.md"),
        }

        categories = ["kafka", "devops", "unknown"]
        for cat in categories:
            msg = {"title": f"{cat} note", "category": cat, "tags": [cat]}
            test_producer.produce(
                TEST_TOPIC,
                key=cat.encode("utf-8"),
                value=json.dumps(msg).encode("utf-8"),
            )
        test_producer.flush()

        for _ in range(3):
            msg = test_consumer.poll(timeout=10.0)
            if msg is None or msg.error():
                continue
            value = json.loads(msg.value().decode("utf-8"))
            category = value.get("category", "_default")
            filepath = routing.get(category, routing["_default"])
            append_note(filepath, title=value["title"], tags=value["tags"])
            test_consumer.commit(message=msg)

        assert os.path.exists(routing["kafka"])
        assert os.path.exists(routing["devops"])
        assert os.path.exists(routing["_default"])
        assert "kafka note" in open(routing["kafka"]).read()
        assert "devops note" in open(routing["devops"]).read()
        assert "unknown note" in open(routing["_default"]).read()

    def test_manual_offset_commit(self, kafka_admin):
        """Offset is only committed after successful file write — at-least-once semantics.

        Uses a dedicated topic so no other test's messages interfere.
        """
        group_id = _unique_group()
        commit_topic = f"test.commit.{uuid.uuid4().hex[:8]}"

        fs = kafka_admin.create_topics([NewTopic(commit_topic, num_partitions=1, replication_factor=1)])
        for t, f in fs.items():
            f.result()

        try:
            p = Producer({"bootstrap.servers": BOOTSTRAP})
            p.produce(commit_topic, key=b"test", value=json.dumps({"title": "Commit Test"}).encode())
            p.flush()

            # First consumer reads but does NOT commit
            c1 = Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            c1.subscribe([commit_topic])
            msg = c1.poll(timeout=10.0)
            assert msg is not None
            # Deliberately skip commit — simulating a file write failure
            c1.close()

            # Second consumer in same group should re-read the uncommitted message
            c2 = Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            c2.subscribe([commit_topic])
            msg2 = c2.poll(timeout=10.0)
            assert msg2 is not None, "Message should be redelivered when offset wasn't committed"
            value = json.loads(msg2.value().decode("utf-8"))
            assert value["title"] == "Commit Test"
            c2.close()
        finally:
            kafka_admin.delete_topics([commit_topic])
