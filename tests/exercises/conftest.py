import uuid
import pytest
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"


def _unique_group():
    return f"exercise-group-{uuid.uuid4().hex[:8]}"


def _unique_topic(prefix="exercise"):
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def kafka_admin():
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


@pytest.fixture
def exercise_topic(kafka_admin):
    """Creates a unique topic per test, deleted on teardown.

    Usage: topic_name = exercise_topic(num_partitions=3)
    Returns a factory function so partition count can vary per test.
    """
    created = []

    def _create(num_partitions=3):
        name = _unique_topic()
        fs = kafka_admin.create_topics([NewTopic(name, num_partitions=num_partitions, replication_factor=1)])
        for t, f in fs.items():
            f.result()
        created.append(name)
        return name

    yield _create

    for name in created:
        fs = kafka_admin.delete_topics([name])
        for t, f in fs.items():
            try:
                f.result()
            except Exception:
                pass


@pytest.fixture
def exercise_topic_with_config(kafka_admin):
    """Creates a topic with custom config (e.g., retention.ms).

    Usage: topic_name = exercise_topic_with_config(config={"retention.ms": "5000"})
    """
    created = []

    def _create(num_partitions=1, config=None):
        name = _unique_topic()
        topic = NewTopic(name, num_partitions=num_partitions, replication_factor=1, config=config or {})
        fs = kafka_admin.create_topics([topic])
        for t, f in fs.items():
            f.result()
        created.append(name)
        return name

    yield _create

    for name in created:
        fs = kafka_admin.delete_topics([name])
        for t, f in fs.items():
            try:
                f.result()
            except Exception:
                pass


@pytest.fixture
def make_producer():
    def _create(**overrides):
        config = {"bootstrap.servers": BOOTSTRAP}
        config.update(overrides)
        return Producer(config)
    return _create


@pytest.fixture
def make_consumer():
    """Factory for consumers. Waits for partition assignment before returning."""
    consumers = []

    def _create(topic, group_id=None, wait_for_assignment=True, **overrides):
        config = {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_id or _unique_group(),
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
        config.update(overrides)
        c = Consumer(config)
        c.subscribe([topic])
        if wait_for_assignment:
            for _ in range(30):
                c.poll(timeout=0.5)
                if c.assignment():
                    break
            else:
                raise RuntimeError("Consumer failed to get partition assignment")
        consumers.append(c)
        return c

    yield _create

    for c in consumers:
        try:
            c.close()
        except Exception:
            pass
