#!/usr/bin/env python3
"""Consumer: Kafka → pCloud markdown files

Reads notes from Kafka and appends them to category-specific markdown files.
Commits offsets only after successful file write (at-least-once delivery).

Usage: python consumer.py
"""

import json
import os
import signal
import logging

from lib.config import load_config
from lib.kafka_helpers import create_consumer
from lib.markdown import append_note

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger(__name__)

running = True


def shutdown(sig, frame):
    global running
    log.info("Shutting down (received %s)...", signal.Signals(sig).name)
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    config = load_config()
    routing = config["routing"]
    default_path = routing.get("_default")

    # Verify at least the default output path is reachable
    for path in routing.values():
        parent = os.path.dirname(path)
        if not os.path.isdir(parent):
            log.warning("Directory %s does not exist — notes for this route will fail", parent)

    consumer = create_consumer(config)
    consumer.subscribe([config["kafka"]["topic"]])
    log.info("Subscribed to '%s'. Waiting for messages... (Ctrl+C to stop)", config["kafka"]["topic"])

    processed = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                key = msg.key().decode("utf-8") if msg.key() else "_default"
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error("Bad message at offset %d: %s", msg.offset(), e)
                consumer.commit(message=msg)
                continue

            category = value.get("category", key)
            filepath = routing.get(category, default_path)

            if not filepath:
                log.warning("No route for category '%s', skipping", category)
                consumer.commit(message=msg)
                continue

            try:
                append_note(
                    filepath,
                    title=value.get("title", "Untitled"),
                    content=value.get("content", ""),
                    tags=value.get("tags"),
                    timestamp=value.get("timestamp"),
                )
                consumer.commit(message=msg)
                processed += 1
                log.info(
                    "Appended to %s: '%s' [partition=%d offset=%d]",
                    os.path.basename(filepath),
                    value.get("title", "?"),
                    msg.partition(),
                    msg.offset(),
                )
            except OSError as e:
                # Don't commit — message will be redelivered on next run
                log.error("File write failed for %s: %s (will retry)", filepath, e)

    finally:
        consumer.close()
        log.info("Consumer closed. Processed %d messages this session.", processed)


if __name__ == "__main__":
    main()
