#!/usr/bin/env bash
# run.sh — Start Kafka, produce from Google Tasks, consume to pCloud
#
# Usage:
#   ./run.sh           # one-shot: fetch, consume, exit
#   ./run.sh --watch   # long-running: producer polls every 5min, consumer stays up

set -e

cd "$(dirname "$0")"

echo "=== Step 1: Starting Kafka infrastructure ==="
docker compose up -d
echo ""

echo "=== Step 2: Waiting for Kafka broker to be ready ==="
until docker exec ks-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo "  Waiting..."
  sleep 2
done
echo "  Kafka is ready."
echo ""

echo "=== Step 3: Creating topic (notes.inbound, 3 partitions) ==="
docker exec ks-kafka kafka-topics --create \
  --topic notes.inbound \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
echo ""

source .venv/bin/activate

if [[ "$1" == "--watch" ]]; then
  echo "=== Watch mode: producer + consumer running. Ctrl+C to stop ==="
  echo ""
  python producer.py --watch &
  PRODUCER_PID=$!
  python consumer.py &
  CONSUMER_PID=$!

  trap "kill $PRODUCER_PID $CONSUMER_PID 2>/dev/null; wait; echo 'Stopped.'" INT TERM
  wait
else
  echo "=== Step 4: Running producer (Google Tasks → Kafka) ==="
  python producer.py
  echo ""

  echo "=== Step 5: Running consumer (Kafka → pCloud) ==="
  python consumer.py
fi
