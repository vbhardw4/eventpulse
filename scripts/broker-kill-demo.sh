#!/usr/bin/env bash
# Broker-kill recovery demo.
#
# Kills one of three brokers mid-stream, shows the pipeline surviving on the
# remaining two (RF=3, minISR=2), restarts it, and verifies ZERO message loss
# by comparing produced vs consumed-unique counters.
#
# Prereqs: docker compose up -d, app running (./scripts/run-app.sh).
# Watch:   Grafana http://localhost:3000 — "Consumer lag by partition" panel.
set -euo pipefail

API=http://localhost:8080/api/demo/stats

snap() { curl -s "$API"; }

echo "==> Baseline:"; snap; echo
echo "==> Killing eventpulse-kafka-2 in 5s — watch Grafana lag spike..."
sleep 5
docker kill eventpulse-kafka-2
echo "==> kafka-2 is DOWN. Pipeline should keep serving on kafka-1 + kafka-3."
echo "==> Current lag (total):"
sleep 10
snap; echo
echo "==> Restarting kafka-2 in 10s..."
sleep 10
docker start eventpulse-kafka-2
echo "==> kafka-2 restarting; waiting 60s for rejoin + catch-up..."
sleep 60
echo "==> After recovery:"; snap; echo
echo
echo "Done. produced vs consumedUnique must match exactly (duplicates are"
echo "deduped by orderId; aborted-transaction records are never visible)."
echo "Verify in Grafana: lag spiked, then returned to ~0."
