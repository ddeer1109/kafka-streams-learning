"""Exercise 2: Delivery Callbacks

WHERE IT LIVES:
  lib/kafka_helpers.py:8-15   — _delivery_callback logs partition + offset
  lib/kafka_helpers.py:32     — producer.flush() triggers pending callbacks

THE CONCEPT:
  producer.produce() is ASYNCHRONOUS — it queues the message in an internal
  buffer and returns immediately. The delivery callback fires later, when
  the broker acknowledges (or rejects) the message.

  Callbacks fire during flush() or poll(), not during produce().
  Your project calls flush() after every message (synchronous pattern).
"""

import json
import pytest
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestDeliveryCallbacks:

    def test_callback_receives_metadata(self, exercise_topic):
        """Baseline: delivery callback gets topic, partition, and offset."""
        topic = exercise_topic(num_partitions=3)

        results = []

        def on_delivery(err, msg):
            results.append({
                "err": err,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key(),
            })

        p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
        p.produce(topic, key=b"mykey", value=b'{"data": 1}', callback=on_delivery)
        p.flush()

        assert len(results) == 1
        r = results[0]
        assert r["err"] is None
        assert r["topic"] == topic
        assert r["partition"] >= 0
        assert r["offset"] >= 0
        assert r["key"] == b"mykey"

    def test_callbacks_fire_on_flush_not_produce(self, exercise_topic):
        """produce() queues messages; callbacks fire during flush().

        PREDICT: After 5 produce() calls but before flush(), how many
        callbacks have fired?
        """
        topic = exercise_topic(num_partitions=1)

        results = []

        def on_delivery(err, msg):
            results.append(msg.offset())

        p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})

        # Produce 5 messages WITHOUT flushing
        for i in range(5):
            p.produce(topic, value=f'{{"seq": {i}}}'.encode(), callback=on_delivery)

        # At this point, callbacks have NOT fired yet
        # (they might on some systems if the internal buffer triggers a send,
        #  but conceptually produce() is async)
        before_flush = len(results)

        # Now flush — this blocks until all messages are delivered
        p.flush()

        after_flush = len(results)

        # All 5 callbacks fire during or after flush
        assert after_flush == 5
        # Most or all callbacks were pending before flush
        assert before_flush < after_flush

    def test_produce_raises_on_oversized_message(self, exercise_topic):
        """When a message is too large, produce() raises immediately.

        PREDICT: Does the error come through the callback, or as an exception?
        (Answer: the producer rejects it before even queuing — it's an exception,
        not a callback. The callback only fires for messages that were queued
        and then failed during delivery.)
        """
        from confluent_kafka import KafkaException

        p = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "acks": "all",
            "message.max.bytes": "1000000",
        })

        # 2MB message — exceeds message.max.bytes
        large_value = b"x" * 2_000_000
        with pytest.raises(KafkaException) as exc_info:
            p.produce("any.topic", value=large_value)

        assert "too large" in str(exc_info.value).lower()
