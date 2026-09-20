package dev.vishalbhardwaj.eventpulse.consumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dev.vishalbhardwaj.eventpulse.avro.OrderEvent;
import dev.vishalbhardwaj.eventpulse.metrics.TimeBuckets;

/**
 * Applies a batch of orders to the aggregates exactly once per business key.
 *
 * <p>Throughput design: a batch of up to {@code max.poll.records} is applied in
 * <b>two Postgres round trips</b> (bulk dedupe insert, bulk revenue upsert) and
 * <b>one Redis pipeline</b>, all inside a single DB transaction — instead of
 * ~7 round trips per record. The {@code INSERT ... ON CONFLICT DO NOTHING} is
 * the idempotency gate: if an {@code orderId} was already applied (crash
 * between DB commit and offset commit, or a business-level resubmission), the
 * business write is skipped. The offset is committed only after this method
 * returns successfully.
 */
@Service
public class RevenueService {

    private static final Logger log = LoggerFactory.getLogger(RevenueService.class);

    private static final String TOTAL_CENTS_KEY = "eventpulse:revenue:total_cents";
    private static final String ORDER_COUNT_KEY = "eventpulse:revenue:order_count";
    private static final String MINUTE_PREFIX = "eventpulse:revenue:minute";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;

    public RevenueService(JdbcTemplate jdbc, StringRedisTemplate redis) {
        this.jdbc = jdbc;
        this.redis = redis;
    }

    /** One order awaiting application, with its Kafka coordinates for the dedupe row. */
    public record PendingOrder(String topic, int partition, long offset, OrderEvent event) {
    }

    /**
     * Applies every order in the batch idempotently.
     *
     * @return the events that were applied for the first time (duplicates
     *         excluded), for latency tracking and stats.
     */
    @Transactional("transactionManager")
    public List<OrderEvent> applyBatch(List<PendingOrder> pending) {
        if (pending.isEmpty()) {
            return List.of();
        }

        // 1. Bulk idempotent insert. batchUpdate returns per-row counts:
        //    1 = first sight of this orderId, 0 = duplicate (ON CONFLICT DO NOTHING).
        int[][] counts = jdbc.batchUpdate(
                "INSERT INTO processed_orders(order_id, topic, record_partition, record_offset, processed_at)"
                        + " VALUES (?,?,?,?,now()) ON CONFLICT (order_id) DO NOTHING",
                pending, pending.size(),
                (ps, p) -> {
                    ps.setString(1, p.event().getOrderId());
                    ps.setString(2, p.topic());
                    ps.setInt(3, p.partition());
                    ps.setLong(4, p.offset());
                });

        // 2. Aggregate the newly-seen orders per minute bucket, then bulk upsert.
        Map<LocalDateTime, long[]> byMinute = new HashMap<>();
        List<OrderEvent> applied = new ArrayList<>();
        for (int i = 0; i < pending.size(); i++) {
            if (counts[i][0] == 1) {
                OrderEvent e = pending.get(i).event();
                applied.add(e);
                long[] agg = byMinute.computeIfAbsent(
                        TimeBuckets.truncateToMinute(e.getProducedAt()), k -> new long[2]);
                agg[0] += e.getAmountCents();
                agg[1] += 1;
            }
        }
        if (!byMinute.isEmpty()) {
            List<Map.Entry<LocalDateTime, long[]>> entries = List.copyOf(byMinute.entrySet());
            jdbc.batchUpdate(
                    "INSERT INTO revenue_minutes(minute, order_count, total_cents) VALUES (?,?,?)"
                            + " ON CONFLICT (minute) DO UPDATE SET"
                            + " order_count = revenue_minutes.order_count + EXCLUDED.order_count,"
                            + " total_cents = revenue_minutes.total_cents + EXCLUDED.total_cents",
                    entries, entries.size(),
                    (ps, en) -> {
                        ps.setObject(1, en.getKey());
                        ps.setLong(2, en.getValue()[1]);
                        ps.setLong(3, en.getValue()[0]);
                    });
        }

        // 3. Redis best-effort mirror of the Postgres totals — one pipelined
        //    round trip for the whole batch. Postgres is the source of truth.
        if (!applied.isEmpty()) {
            try {
                redis.executePipelined(new SessionCallback<List<Object>>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public List<Object> execute(RedisOperations operations) {
                        ValueOperations<String, String> v = operations.opsForValue();
                        for (OrderEvent e : applied) {
                            v.increment(TOTAL_CENTS_KEY, e.getAmountCents());
                            v.increment(ORDER_COUNT_KEY, 1);
                            String minuteKey = TimeBuckets.minuteKey(MINUTE_PREFIX, e.getProducedAt()) + ":cents";
                            v.increment(minuteKey, e.getAmountCents());
                            operations.expire(minuteKey, Duration.ofHours(2));
                        }
                        return null;
                    }
                });
            } catch (Exception e) {
                log.debug("Redis pipeline failed, Postgres totals unaffected: {}", e.toString());
            }
        }
        return applied;
    }
}
