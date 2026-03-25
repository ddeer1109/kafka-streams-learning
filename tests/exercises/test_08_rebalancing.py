"""Exercise 8: Consumer Rebalancing

WHERE IT LIVES:
  consumer.py:50  — consumer.subscribe([topic]) — currently has NO rebalance callback

THE CONCEPT:
  When consumers join or leave a group, Kafka redistributes partitions among
  the remaining consumers. This is called a REBALANCE.

  You can hook into this with on_assign and on_revoke callbacks:
  - on_revoke: "you're about to lose these partitions" (flush state, commit offsets)
  - on_assign: "you now own these partitions" (initialize state)

  Your project doesn't use these callbacks yet. This exercise shows what
  they do and why they matter for stateful consumers.
"""

import json
import time
import threading
import uuid
import pytest
from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestRebalancing:

    def test_rebalance_callbacks_fire_on_join(self, exercise_topic):
        """Adding a second consumer triggers on_revoke then on_assign."""
        topic = exercise_topic(num_partitions=3)
        group = f"rebalance-{uuid.uuid4().hex[:8]}"

        assign_log = []
        revoke_log = []

        def on_assign(consumer, partitions):
            assign_log.append([tp.partition for tp in partitions])

        def on_revoke(consumer, partitions):
            revoke_log.append([tp.partition for tp in partitions])

        # Consumer 1 joins with callbacks
        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

        # Wait for initial assignment (c1 gets all 3 partitions)
        for _ in range(30):
            c1.poll(timeout=0.5)
            if c1.assignment():
                break

        assert len(assign_log) >= 1
        initial_partitions = set(assign_log[-1])
        assert initial_partitions == {0, 1, 2}

        # Consumer 2 joins — triggers rebalance
        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])

        # Poll both to allow rebalance to happen
        for _ in range(30):
            c1.poll(timeout=0.5)
            c2.poll(timeout=0.5)
            if c2.assignment():
                break

        # on_revoke should have fired (c1 gave up some partitions)
        assert len(revoke_log) >= 1, "on_revoke should fire during rebalance"

        # After rebalance, c1 has fewer partitions
        c1_parts = {tp.partition for tp in c1.assignment()}
        c2_parts = {tp.partition for tp in c2.assignment()}
        assert c1_parts | c2_parts == {0, 1, 2}
        assert len(c1_parts) < 3  # c1 gave up at least one

        c1.close()
        c2.close()

    def test_rebalance_on_consumer_leave(self, exercise_topic):
        """Closing a consumer triggers rebalance — remaining consumer gets all partitions.

        PREDICT: After c2 leaves, how many partitions does c1 get back?
        """
        topic = exercise_topic(num_partitions=3)
        group = f"rebal-leave-{uuid.uuid4().hex[:8]}"

        reassignments = []

        def on_assign(consumer, partitions):
            reassignments.append(("assign", [tp.partition for tp in partitions]))

        def on_revoke(consumer, partitions):
            reassignments.append(("revoke", [tp.partition for tp in partitions]))

        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
        for _ in range(30):
            c1.poll(timeout=0.5)
            if c1.assignment():
                break

        c2 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c2.subscribe([topic])
        for _ in range(30):
            c1.poll(timeout=0.5)
            c2.poll(timeout=0.5)
            if c2.assignment():
                break

        # Now close c2 — triggers rebalance, c1 gets all partitions back
        reassignments.clear()
        c2.close()

        for _ in range(30):
            c1.poll(timeout=0.5)
            c1_parts = {tp.partition for tp in c1.assignment()}
            if c1_parts == {0, 1, 2}:
                break

        c1_parts = {tp.partition for tp in c1.assignment()}
        assert c1_parts == {0, 1, 2}, f"c1 should have all partitions back, got {c1_parts}"

        c1.close()
