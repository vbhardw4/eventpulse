#!/usr/bin/env bash
# Builds (if needed) and runs the EventPulse app against the compose stack.
# Usage: ./scripts/run-app.sh [--load]   (--load runs the one-shot load test)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f target/eventpulse-0.1.0.jar ]; then
  echo "==> Building jar (first run)..."
  mvn -q -DskipTests package
fi

PROFILE_ARGS=()
if [ "${1:-}" = "--load" ]; then
  PROFILE_ARGS=(--spring.profiles.active=load)
  shift || true
fi

echo "==> Starting EventPulse app..."
exec java -jar target/eventpulse-0.1.0.jar "${PROFILE_ARGS[@]}" "$@"
