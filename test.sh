#!/usr/bin/env bash
set -euo pipefail

# Test runner for Knowledge Stream Hub
#
# Usage:
#   ./test.sh              # unit tests only (no dependencies)
#   ./test.sh --integration # all tests (requires Kafka broker)
#   ./test.sh --exercises   # Kafka concept exercises (requires Kafka broker)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

source .venv/bin/activate

if [[ " $* " == *" --integration "* ]] || [[ " $* " == *" --exercises "* ]]; then
    echo "==> Starting Kafka infrastructure..."
    docker compose up -d kafka
    echo "==> Waiting for broker to be ready..."
    for i in $(seq 1 30); do
        if docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
            echo "==> Broker ready."
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo "ERROR: Kafka broker didn't start in time."
            exit 1
        fi
        sleep 1
    done
fi

if [[ " $* " == *" --exercises "* ]]; then
    echo "==> Running exercises..."
    # Strip --exercises from args, pass --integration to pytest
    ARGS="${@/--exercises/}"
    python -m pytest tests/exercises/ -v --integration $ARGS
else
    echo "==> Running tests..."
    python -m pytest tests/ -v "$@"
fi
