# Knowledge Stream Hub

## Project context

A real productivity tool that doubles as a Kafka learning project. Google Tasks → Kafka → pCloud markdown notebooks.

Current focus: **core Kafka mechanics** (producer/consumer API, serialization, partitions, consumer groups, offsets). Kafka Streams deferred to a stream processing framework later.

## Architecture

- `producer.py` — polls Google Tasks "Kafka Inbox" list, publishes to `notes.inbound` topic
- `consumer.py` — reads topic, routes by `#category` hashtag to markdown files in `/mnt/p/KnowledgeHub/`
- `lib/` — shared modules (config, kafka helpers, Google Tasks API, markdown writer)
- `run.sh` — orchestrator: starts Kafka, runs producer + consumer (supports `--watch` mode)
- `docker-compose.yml` — Zookeeper + Kafka broker + Kafka UI (localhost:8090)

## Teaching approach

When I ask about Kafka concepts, **tie explanations to this project's code** — show me where the concept lives in producer.py, consumer.py, or config.yml rather than abstract examples. I learn by connecting theory to the running system.

Key concepts to reinforce as they come up naturally:
- Producer: acks, delivery callbacks, key-based partitioning, serialization
- Consumer: poll loop, manual offset commit, consumer groups, rebalancing
- Broker: topics, partitions, replication, retention
- Operational: consumer lag, offset management, exactly-once vs at-least-once

## Conventions

- Python, no frameworks — plain `confluent-kafka` client
- Config in `config.yml`, secrets in `.env`
- Google OAuth credentials via `client_secret.json` (gitignored)
- pCloud notebooks at `/mnt/p/KnowledgeHub/*.md`
- Category routing is config-driven in `config.yml` under `routing:`

## What not to do

- Don't introduce Kafka Streams or ksqlDB — that's for the a stream processing framework phase
- Don't add web frameworks (Flask, FastAPI) unless explicitly asked
- Don't over-abstract — two flat scripts + a lib folder is the right level
