import json
import logging
from confluent_kafka import Producer, Consumer

log = logging.getLogger(__name__)


def _delivery_callback(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        log.info(
            "Delivered to %s [partition %d] @ offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def create_producer(config):
    return Producer({
        "bootstrap.servers": config["kafka"]["bootstrap_servers"],
        "acks": str(config["kafka"]["acks"]),
    })


def produce_note(producer, topic, key, value, callback=None):
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        callback=callback or _delivery_callback,
    )
    producer.flush()


def create_consumer(config):
    return Consumer({
        "bootstrap.servers": config["kafka"]["bootstrap_servers"],
        "group.id": config["kafka"]["consumer_group"],
        "auto.offset.reset": config["kafka"]["auto_offset_reset"],
        "enable.auto.commit": config["kafka"]["enable_auto_commit"],
    })
