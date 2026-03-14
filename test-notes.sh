#!/usr/bin/env bash
# test-notes.sh — Send sample notes to the Knowledge Stream Hub
# Usage: ./test-notes.sh

BASE="http://localhost:8080/api"

post_note() {
  local title="$1" content="$2" domain="$3" tags="$4"
  echo ">>> Posting: $title [$domain]"
  curl -s -X POST "$BASE/notes" \
    -H "Content-Type: application/json" \
    -d "{\"title\":\"$title\",\"content\":\"$content\",\"domain\":\"$domain\",\"tags\":[$tags]}" \
    | python3 -m json.tool 2>/dev/null || echo "(raw response above)"
  echo ""
}

echo "=== Sending test notes ==="
echo ""

post_note "Docker Networking" \
  "Bridge vs overlay networks in Docker" \
  "devops" \
  '"docker","networking"'

post_note "Spring Boot Profiles" \
  "Using profiles for env-specific configuration" \
  "fullstack" \
  '"spring","java","config"'

post_note "Sidechain Compression" \
  "Using sidechain to duck bass under kick" \
  "music" \
  '"mixing","compression"'

post_note "RAG Pipeline Basics" \
  "Retrieval augmented generation with embeddings" \
  "ai-context" \
  '"rag","embeddings","llm"'

post_note "Kafka Consumer Groups" \
  "Partition assignment and rebalancing" \
  "devops" \
  '"kafka","streaming"'

echo "=== Checking stats ==="
echo ""
curl -s "$BASE/stats" | python3 -m json.tool 2>/dev/null
