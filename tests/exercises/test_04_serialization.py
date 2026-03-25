"""Exercise 4: Message Serialization

WHERE IT LIVES:
  lib/kafka_helpers.py:29  — json.dumps(value).encode("utf-8") (producer side)
  consumer.py:65           — json.loads(msg.value().decode("utf-8")) (consumer side)
  consumer.py:67-69        — JSONDecodeError / UnicodeDecodeError handling

THE CONCEPT:
  Kafka messages are raw bytes. Your application is responsible for
  serialization (Python dict → bytes) and deserialization (bytes → dict).

  Your project uses JSON + UTF-8 encoding. This is simple and human-readable,
  but has no schema enforcement — a bad message will only fail at consume time.
  Production systems often use Avro or Protobuf with a schema registry.
"""

import json
import pytest
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestSerialization:

    def test_json_round_trip(self, exercise_topic, make_consumer):
        """Baseline: JSON dict survives the produce → consume round-trip."""
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        original = {"title": "Test Note", "tags": ["kafka", "devops"], "count": 42}

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"test", value=json.dumps(original).encode("utf-8"))
        p.flush()

        msg = consumer.poll(timeout=10.0)
        assert msg is not None
        decoded = json.loads(msg.value().decode("utf-8"))
        assert decoded == original

    def test_invalid_json_raises_error(self, exercise_topic, make_consumer):
        """Producing non-JSON bytes — consumer gets garbage.

        PREDICT: What exception does json.loads() raise on invalid input?
        This is what consumer.py:67 catches.
        """
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        # Produce raw bytes that aren't valid JSON
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key=b"bad", value=b"this is not json {{{")
        p.flush()

        msg = consumer.poll(timeout=10.0)
        assert msg is not None

        # Kafka delivered the bytes fine — it doesn't care about format
        # But our application can't parse it
        with pytest.raises(json.JSONDecodeError):
            json.loads(msg.value().decode("utf-8"))

    def test_unicode_round_trip(self, exercise_topic, make_consumer):
        """UTF-8 handles international characters.

        PREDICT: Will emoji and non-Latin characters survive the round-trip?
        """
        topic = exercise_topic(num_partitions=1)
        consumer = make_consumer(topic)

        original = {"title": "Notizen auf Deutsch", "content": "Umlaute: ä ö ü ß"}

        p = Producer({"bootstrap.servers": BOOTSTRAP})
        p.produce(topic, key="deutsch".encode("utf-8"), value=json.dumps(original).encode("utf-8"))
        p.flush()

        msg = consumer.poll(timeout=10.0)
        assert msg is not None
        decoded = json.loads(msg.value().decode("utf-8"))
        assert decoded["content"] == "Umlaute: ä ö ü ß"
        assert msg.key().decode("utf-8") == "deutsch"
