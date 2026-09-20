# EventPulse Runbook & Handover

Who this is for: the client's engineer who inherits this pipeline. If you can
run Docker Compose and read a Grafana dashboard, you can operate this.

## 1. Normal operation

```bash
docker compose up -d          # infra: Kafka x3, Schema Registry, Postgres, Redis, Prometheus, Grafana
./scripts/run-app.sh          # the pipeline app (builds the jar on first run)
```

Dashboards:
- Grafana: http://localhost:3000 (admin/admin) → "EventPulse — real-time pipeline"
- Prometheus: http://localhost:9090 · App health: http://localhost:8080/actuator/health

## 2. What to watch (the 4 signals that matter)

| Signal | Where | Healthy | Act when |
|--------|-------|---------|----------|
| Consumer lag by partition | Grafana | ~0, brief spikes OK | > 5,000 for > 1 min (warning), > 50,000 (critical) |
| End-to-end p99 latency | Grafana | < 2 s on demo hardware | sustained > 2 s — check downstream (Postgres/Redis), not Kafka first |
| DLQ pending | Grafana stat / `GET /api/dlq/pending` | 0 | any growth — inspect the quarantined payload |
| Under-replicated partitions | Kafka (add JMX exporter — see §6) | 0 | > 0 for > 5 min — a broker is unhealthy |

**Important diagnostic habit:** when lag grows, check the *consumer's downstream*
(Postgres slow? Redis down?) before tuning Kafka. In most incidents Kafka is the
messenger, not the culprit.

## 3. Incident playbooks

### Consumer lag climbing
1. `GET /api/demo/stats` — is `consumed` still increasing? If yes, it's slowness, not a stall.
2. Check Postgres: `docker compose exec postgres psql -U eventpulse -c "SELECT count(*) FROM pg_stat_activity;"` — connection exhaustion shows here first.
3. Check for a rebalance storm in app logs: `rebalance` + `Revoked partitions` repeating.
4. Mitigations in order: raise `eventpulse.topics.orders-partitions` (requires topic edit),
   raise listener `concurrency`, then scale app instances (same `group.id`).

### Poison messages in DLQ
1. `GET /api/dlq/pending` — read `exception` + `sourceTopic`.
2. Fix the producer bug that emitted the bad payload.
3. `POST /api/dlq/replay` — republishes original bytes. If the bug isn't fixed, the
   record lands back in the DLQ (visible, not lost).

### A broker dies
1. Don't panic: RF=3 / minISR=2 means the cluster keeps serving on two brokers.
2. `docker start eventpulse-kafka-2` (or replace the host in production).
3. Watch Grafana lag fall back to ~0; verify `produced == consumedUnique` via
   `GET /api/demo/stats`.
4. Investigate the host *after* recovery, not during.

### Schema Registry down
Producers fail fast on new schemas; existing consumers are unaffected (they cache
schemas). Restart the container; no data path depends on it at steady state.

## 4. Configuration reference

All knobs live in `src/main/resources/application.yml` under `eventpulse.*`:

| Key | Default | Notes |
|-----|---------|-------|
| `eventpulse.topics.orders-partitions` | 12 | Raise before raising consumer concurrency |
| `eventpulse.topics.replication-factor` | 3 | Never 1 in production |
| `eventpulse.consumer.lag-alert-threshold` | 5000 | Grafana/prometheus alerts match this |
| `eventpulse.simulator.rate-per-second` | 200 | Steady background load for the demo |

Secrets: the demo uses Postgres `trust` auth so the repo contains zero secrets.
In production, set real passwords via compose env vars and move them to a
secrets manager (AWS Secrets Manager / Vault).

## 5. Handover checklist (what the client receives)

- [ ] This repo, with the client-specific topic names and retention configured
- [ ] Grafana dashboard JSON (in `grafana/dashboards/`) imported to their Grafana
- [ ] Prometheus alert rules (`prometheus/alerts.yml`) wired to their Alertmanager →
      PagerDuty/OpsGenie/Slack
- [ ] Runbook walkthrough call (60 min): kill-a-broker drill done live together
- [ ] Schema Registry compatibility policy documented (BACKWARD on all value subjects)
- [ ] Credentials rotated; all logins owned by the client

## 6. Production hardening (beyond this demo)

- 3+ brokers across availability zones (or Confluent Cloud / AWS MSK — managed is
  usually the right call; the audit tells you which)
- JMX exporter on brokers → Prometheus (under-replicated partitions, ISR shrink,
  request latency) — the single most-missed monitoring in Kafka incidents
- Alertmanager wired to the rules in `prometheus/alerts.yml`
- Topic retention + compaction policy per topic; schema-compatibility CI check on
  every schema change
- Terraform for the whole stack; backups for Schema Registry + Postgres
- mTLS/SASL between clients and brokers; network policies

## 7. What this demo deliberately does NOT include

Single-machine Docker Compose, PLAINTEXT listeners, default passwords, no auth on
Grafana, no Alertmanager delivery. Every one of these is a conscious demo shortcut
— §6 lists the production replacements. Saying this out loud is part of the handover.
