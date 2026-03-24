import os
from lib.config import load_config


def test_load_config_parses_yaml(tmp_path):
    cfg_file = tmp_path / "config.yml"
    cfg_file.write_text("""
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "test.topic"
routing:
  _default: "/tmp/default.md"
""")
    config = load_config(str(cfg_file))
    assert config["kafka"]["bootstrap_servers"] == "localhost:9092"
    assert config["kafka"]["topic"] == "test.topic"
    assert config["routing"]["_default"] == "/tmp/default.md"


def test_load_config_adds_google_oauth(tmp_path):
    cfg_file = tmp_path / "config.yml"
    cfg_file.write_text("kafka:\n  topic: test\n")
    os.environ["GOOGLE_CLIENT_ID"] = "test-id"
    os.environ["GOOGLE_CLIENT_SECRET"] = "test-secret"
    try:
        config = load_config(str(cfg_file))
        assert config["google_oauth"]["client_id"] == "test-id"
        assert config["google_oauth"]["client_secret"] == "test-secret"
    finally:
        del os.environ["GOOGLE_CLIENT_ID"]
        del os.environ["GOOGLE_CLIENT_SECRET"]
