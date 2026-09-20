#!/usr/bin/env bash
# One-shot throughput/latency measurement.
# Prereqs: docker compose up -d  (infra running), jar built.
# Usage: ./scripts/measure.sh [count] [rate-per-second]   (defaults: 600000 10000)
#
# Prints the load-test report and exits non-zero if any message was lost.
set -euo pipefail
cd "$(dirname "$0")/.."

COUNT="${1:-600000}"
RATE="${2:-10000}"

if [ ! -f target/eventpulse-0.1.0.jar ]; then
  echo "==> Building jar..."
  mvn -q -DskipTests package
fi

echo "==> Measuring: $COUNT events at $RATE/sec"
java -jar target/eventpulse-0.1.0.jar \
  --spring.profiles.active=load \
  --eventpulse.load.count="$COUNT" \
  --eventpulse.load.rate-per-second="$RATE"
