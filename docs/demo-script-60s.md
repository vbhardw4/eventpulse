# EventPulse — 60-second demo script

> **Vishal's step:** record this yourself (Loom or phone-over-screen). The commands
> below are the exact sequence. Keep the Grafana dashboard visible throughout.

**Setup before recording:** `docker compose up -d`, `./scripts/run-app.sh`,
open Grafana http://localhost:3000 (admin/admin) → "EventPulse — real-time pipeline".

| Time | Say | Do |
|------|-----|----|
| 0:00–0:10 | "This is EventPulse — a production-pattern Kafka pipeline. Three brokers, Schema Registry, Postgres sink, Redis aggregates, Grafana. The load simulator is emitting 200 orders a second." | Show Grafana: revenue-per-minute climbing. |
| 0:10–0:20 | "Every order is processed exactly once. Watch — I'll send the same order five times." | `curl -X POST localhost:8080/api/demo/duplicate?count=5` → show `/api/demo/stats`: `consumed` jumps by 5, `revenueOrderCount` moves by exactly 1. |
| 0:20–0:30 | "Poison records don't kill the consumer — they're quarantined with the raw bytes, inspectable, replayable." | `curl -X POST localhost:8080/api/demo/poison` → `curl localhost:8080/api/dlq/pending` shows it. |
| 0:30–0:45 | **The money shot.** "Now I kill a broker mid-stream." | `docker kill eventpulse-kafka-2` → Grafana lag panel spikes, then falls as the cluster keeps serving on two brokers. `docker start eventpulse-kafka-2`. |
| 0:45–0:55 | "Zero loss. Produced versus consumed-unique match exactly." | `curl localhost:8080/api/demo/stats` — show `produced` == `consumedUnique`. |
| 0:55–1:00 | "Schemas evolve without breaking consumers — v2 rolls out live." | `curl -X POST localhost:8080/api/demo/schema-version/v2` → dashboard keeps moving. |

**Close (one line):** "This is the pipeline pattern I run at a bank — the same failure handling, in a demo you can run in three commands."
