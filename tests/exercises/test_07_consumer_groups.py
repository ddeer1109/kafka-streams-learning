"""Exercise 7: Consumer Groups

WHERE IT LIVES:
  config.yml:4             — consumer_group: "notes-to-pcloud"
  lib/kafka_helpers.py:38  — "group.id": config["kafka"]["consumer_group"]

THE CONCEPT:
  A consumer group is a set of consumers that cooperate to consume a topic.
  Kafka distributes partitions among consumers in the same group:

  - 3 partitions, 1 consumer → consumer gets all 3
  - 3 partitions, 2 consumers → one gets 2, other gets 1
  - 3 partitions, 3 consumers → one each
  - 3 partitions, 4 consumers → one consumer is idle (no partition)

  Consumers in DIFFERENT groups are independent — each group gets a full
  copy of all messages. This is how you'd add a second pipeline (e.g.,
  analytics) without affecting the pCloud writer.
"""

import json
import time
import uuid
import pytest
from confluent_kafka import Producer, Consumer

BOOTSTRAP = "localhost:9092"


@pytest.mark.integration
class TestConsumerGroups:

    def test_single_consumer_gets_all_partitions(self, exercise_topic, make_consumer):
        """Baseline: one consumer in a group gets all partitions."""
        topic = exercise_topic(num_partitions=3)
        consumer = make_consumer(topic)

        assignment = consumer.assignment()
        assigned_partitions = {tp.partition for tp in assignment}

        assert assigned_partitions == {0, 1, 2}

    def test_two_consumers_share_partitions(self, exercise_topic):
        """Two consumers in the same group split the partitions.

        PREDICT: With 3 partitions and 2 consumers, how are partitions split?
        """
        topic = exercise_topic(num_partitions=3)
        group = f"shared-{uuid.uuid4().hex[:8]}"

        c1 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c1.subscribe([topic])
        # Wait for c1 to get initial assignment
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
        # Wait for rebalance to settle
        for _ in range(30):
            c1.poll(timeout=0.5)
            c2.poll(timeout=0.5)
            if c2.assignment():
                break

        c1_parts = {tp.partition for tp in c1.assignment()}
        c2_parts = {tp.partition for tp in c2.assignment()}

        # Together they cover all partitions with no overlap
        assert c1_parts | c2_parts == {0, 1, 2}
        assert c1_parts & c2_parts == set()  # no overlap
        assert len(c1_parts) > 0
        assert len(c2_parts) > 0

        c1.close()
        c2.close()

    def test_different_groups_get_independent_copies(self, exercise_topic):
        """Two consumers in different groups each get ALL messages.

        PREDICT: If you produce 5 messages, how many does each group see?
        """
        topic = exercise_topic(num_partitions=1)
        group_a = f"group-a-{uuid.uuid4().hex[:8]}"
        group_b = f"group-b-{uuid.uuid4().hex[:8]}"

        # Both consumers subscribe before producing
        ca = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_a,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        ca.subscribe([topic])
        for _ in range(30):
            ca.poll(timeout=0.5)
            if ca.assignment():
                break

        cb = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_b,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        cb.subscribe([topic])
        for _ in range(30):
            cb.poll(timeout=0.5)
            if cb.assignment():
                break

        # Produce 5 messages
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(5):
            p.produce(topic, key=b"test", value=json.dumps({"seq": i}).encode())
        p.flush()

        # Each group independently consumes all 5
        msgs_a = []
        msgs_b = []
        for _ in range(5):
            msg = ca.poll(timeout=10.0)
            if msg and not msg.error():
                msgs_a.append(msg)
        for _ in range(5):
            msg = cb.poll(timeout=10.0)
            if msg and not msg.error():
                msgs_b.append(msg)

        ca.close()
        cb.close()

        # Both groups got all 5 messages — independent consumption
        assert len(msgs_a) == 5
        assert len(msgs_b) == 5
