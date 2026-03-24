import json
from unittest.mock import MagicMock, call
from lib.kafka_helpers import produce_note, create_producer, create_consumer


def test_produce_note_serializes_json():
    mock_producer = MagicMock()
    value = {"title": "Test", "category": "kafka"}

    produce_note(mock_producer, "notes.inbound", "kafka", value)

    mock_producer.produce.assert_called_once()
    call_args = mock_producer.produce.call_args
    # topic is first positional arg
    assert call_args.args[0] == "notes.inbound"
    assert call_args.kwargs["key"] == b"kafka"
    assert json.loads(call_args.kwargs["value"]) == value
    mock_producer.flush.assert_called_once()


def test_produce_note_custom_callback():
    mock_producer = MagicMock()
    custom_cb = MagicMock()

    produce_note(mock_producer, "topic", "key", {"data": 1}, callback=custom_cb)

    args = mock_producer.produce.call_args
    assert args.kwargs["callback"] is custom_cb


def test_create_producer_config():
    config = {
        "kafka": {
            "bootstrap_servers": "broker:9092",
            "acks": "all",
        }
    }
    # This will fail without a real broker, but we can verify it doesn't crash on config parsing
    # For a true unit test, we'd mock the Producer constructor
    producer = create_producer(config)
    assert producer is not None


def test_create_consumer_config():
    config = {
        "kafka": {
            "bootstrap_servers": "broker:9092",
            "consumer_group": "test-group",
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
        }
    }
    consumer = create_consumer(config)
    assert consumer is not None
