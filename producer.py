#!/usr/bin/env python3
"""Producer: Google Tasks → Kafka

All tasks in the "Kafka Inbox" list get published. Use #category hashtags
to route notes to specific notebooks. Tasks with only hashtags (no real
content) are skipped.

Usage:
  python producer.py            # one-shot: fetch, publish, exit
  python producer.py --watch    # poll every 5 minutes until Ctrl+C
"""

import argparse
import signal
import time
import logging
from datetime import datetime, timezone

from lib.config import load_config
from lib.kafka_helpers import create_producer, produce_note
from lib.google_tasks import get_service, find_task_list, fetch_tasks, parse_tags, complete_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger(__name__)

WATCH_INTERVAL = 300  # 5 minutes

running = True


def shutdown(sig, frame):
    global running
    log.info("Shutting down (received %s)...", signal.Signals(sig).name)
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def fetch_and_publish(service, list_id, producer, topic):
    tasks = fetch_tasks(service, list_id)
    if not tasks:
        log.info("No new tasks found.")
        return 0

    published = 0
    skipped = 0
    for task in tasks:
        title = task.get("title", "")
        notes = task.get("notes", "")
        full_text = f"{title} {notes}"

        clean_title, category, tags = parse_tags(full_text)

        # Skip tasks that are only hashtags with no real content
        if not clean_title and not notes.strip():
            log.info("Skipping empty task (only hashtags): '%s'", title)
            complete_task(service, list_id, task["id"])
            skipped += 1
            continue

        if not category:
            category = "_default"

        message = {
            "title": clean_title or "Untitled",
            "content": notes.strip() if notes else "",
            "category": category,
            "tags": tags,
            "source": "google_tasks",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "google_task_id": task["id"],
        }

        try:
            produce_note(producer, topic, key=category, value=message)
            complete_task(service, list_id, task["id"])
            published += 1
            log.info("Published: '%s' → [%s]", clean_title, category)
        except Exception as e:
            log.error("Failed to publish '%s': %s", clean_title, e)

    if skipped:
        log.info("Skipped %d empty tasks.", skipped)
    return published


def main():
    parser = argparse.ArgumentParser(description="Google Tasks → Kafka producer")
    parser.add_argument("--watch", action="store_true", help="Poll every 5 minutes until Ctrl+C")
    args = parser.parse_args()

    config = load_config()

    service = get_service(config["google_oauth"]["token_path"])
    list_id = find_task_list(service, config["google_tasks"]["list_name"])
    if not list_id:
        log.error("Task list '%s' not found. Create it in Google Tasks first.", config["google_tasks"]["list_name"])
        return

    producer = create_producer(config)
    topic = config["kafka"]["topic"]

    if args.watch:
        log.info("Watch mode: polling every %ds. Ctrl+C to stop.", WATCH_INTERVAL)
        while running:
            fetch_and_publish(service, list_id, producer, topic)
            for _ in range(WATCH_INTERVAL):
                if not running:
                    break
                time.sleep(1)
        log.info("Producer stopped.")
    else:
        total = fetch_and_publish(service, list_id, producer, topic)
        log.info("Done. Published %d notes.", total)


if __name__ == "__main__":
    main()
