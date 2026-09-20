# Kafka Pipeline Audit Checklist

The checklist I run through on a paid Kafka audit engagement ($1,500 fixed,
2–3 days, written report + prioritized fix list). Steal it — if you find
problems you can't fix alone, that's what the engagement is for.

## 1. Producer correctness

- [ ] `enable.idempotence=true` on every producer that must not duplicate
      (and you know it only covers a single producer session — a restart can
      still duplicate without transactions)
- [ ] `acks=all`, `retries=Integer.MAX_VALUE`, `max.in.flight.requests.per.connection ≤ 5`
- [ ] If you claim exactly-once: transactional producer with a stable
      `transactional.id`, and you can explain what the transaction boundary covers
- [ ] Serializer failures are handled (a poison record must not wedge the producer
      thread or silently drop the batch)

## 2. Consumer correctness

- [ ] `isolation.level=read_committed` if any producer is transactional —
      otherwise you read aborted messages
- [ ] Offset commits happen **after** the side effect succeeds, not before
      (commit-before-process = silent data loss on crash)
- [ ] You can state your actual delivery semantics: at-most-once, at-least-once,
      or effectively-once-via-idempotency — and where the idempotency key lives
- [ ] Rebalance behavior: no duplicate processing storms on deploy
      (`max.poll.interval.ms` vs. your slowest record)
- [ ] Deserialization failures go to a DLQ, not into an infinite retry loop

## 3. Exactly-once, stated precisely

- [ ] You can draw the transaction boundary: what is atomic with what
- [ ] Kafka transactions alone only cover Kafka→Kafka. If you write to a
      database, the offset commit is **not** part of that transaction —
      you need business-key dedupe on the sink side (and you have it)
- [ ] The dedupe key is a real business key (`orderId`), not a Kafka offset
      (offsets change on replays and compaction)

## 4. Schema governance

- [ ] Schema Registry in use; compatibility mode is a conscious choice
      (BACKWARD vs FORWARD vs FULL — and you know which your consumers need)
- [ ] No `specific.avro.reader` mismatches between producer and consumer
- [ ] A bad schema can't be registered by accident (CI check or registry ACLs)

## 5. Dead letters

- [ ] Every consumer has a DLQ route for poison records
- [ ] DLQ records keep the original bytes + headers (topic, partition, offset,
      exception) — a DLQ you can't inspect is a second trash can
- [ ] Someone actually looks at the DLQ: alert on growth, runbook for replay

## 6. Lag & alerting

- [ ] Per-partition consumer lag is a metric, with an alert threshold and an owner
- [ ] You know your p99 end-to-end latency and what "bad" looks like for your SLA
- [ ] Broker disk, under-replicated partitions, and offline partitions are alerted
- [ ] Alerts go somewhere a human reads (not a Slack channel nobody watches)

## 7. Failure drills (the ones buyers never run)

- [ ] Kill a broker mid-load: zero loss, consumer group rebalances, producer
      retries — measured, not assumed
- [ ] Rolling restart of consumers: no duplicate storm, lag recovers
- [ ] Schema Registry down: producers fail fast and visibly, not silently

## 8. Operations

- [ ] Replication factor ≥ 3, `min.insync.replicas=2` on anything that matters
- [ ] Topic configs (retention, compaction, segment size) are set deliberately,
      not left at broker defaults
- [ ] You can replay a topic range to rebuild a downstream projection
- [ ] Runbook exists and someone other than the author has followed it

## 9. Security & cost

- [ ] No PLAINTEXT listeners outside a dev sandbox; SASL/SCRAM or mTLS in prod
- [ ] No passwords in repos or compose files (this demo uses trust auth locally
      and says so loudly — production uses a secrets manager)
- [ ] You know what the cluster costs per month and what you'd change first
      (retention, partition count, instance size)

---

*Scoring: every unchecked box is either accepted risk (write down why) or
audit findings. Most teams I talk to check about half. The other half is the
$1,500.*
