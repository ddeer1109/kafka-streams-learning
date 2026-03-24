import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires running Kafka broker")


def pytest_addoption(parser):
    parser.addoption(
        "--integration", action="store_true", default=False,
        help="Run integration tests (requires docker compose up -d)",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--integration"):
        skip = pytest.mark.skip(reason="needs --integration flag and running Kafka broker")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
