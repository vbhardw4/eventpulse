package dev.vishalbhardwaj.eventpulse.consumer;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dev.vishalbhardwaj.eventpulse.avro.OrderEvent;
import dev.vishalbhardwaj.eventpulse.metrics.TimeBuckets;
import dev.vishalbhardwaj.eventpulse.repo.ProcessedOrderRepository;
import dev.vishalbhardwaj.eventpulse.repo.RevenueMinuteRepository;

/**
 * Applies an order to the aggregates exactly once per business key.
 *
 * <p>The {@code INSERT ... ON CONFLICT DO NOTHING} is the idempotency gate: if
 * this {@code orderId} was already applied (crash between DB commit and offset
 * commit, or a business-level resubmission), the business write is skipped.
 * Everything happens in one DB transaction; the offset is committed only after
 * this method returns successfully.
 */
@Service
public class RevenueService {

    private static final Logger log = LoggerFactory.getLogger(RevenueService.class);

    private final ProcessedOrderRepository processedOrders;
    private final RevenueMinuteRepository revenueMinutes;
    private final StringRedisTemplate redis;

    public RevenueService(ProcessedOrderRepository processedOrders,
                          RevenueMinuteRepository revenueMinutes,
                          StringRedisTemplate redis) {
        this.processedOrders = processedOrders;
        this.revenueMinutes = revenueMinutes;
        this.redis = redis;
    }

    /**
     * @return {@code true} if the order was applied, {@code false} if it was a
     *         duplicate and skipped.
     */
    @Transactional
    public boolean applyIfNew(String topic, int partition, long offset, OrderEvent event) {
        int inserted = processedOrders.insertIgnore(
                event.getOrderId(), topic, partition, offset);
        if (inserted == 0) {
            return false;
        }
        revenueMinutes.upsert(
                TimeBuckets.truncateToMinute(event.getProducedAt()),
                event.getAmountCents());

        // Real-time counters in Redis (best-effort mirror of the Postgres totals).
        String minuteKey = TimeBuckets.minuteKey("eventpulse:revenue:minute", event.getProducedAt());
        try {
            redis.opsForValue().increment("eventpulse:revenue:total_cents", event.getAmountCents());
            redis.opsForValue().increment("eventpulse:revenue:order_count", 1);
            Long minuteTotal = redis.opsForValue().increment(minuteKey + ":cents", event.getAmountCents());
            if (minuteTotal != null && minuteTotal == event.getAmountCents()) {
                redis.expire(minuteKey + ":cents", Duration.ofHours(2));
            }
        } catch (Exception e) {
            // Redis is a best-effort mirror; Postgres is the source of truth.
            log.debug("Redis increment failed, Postgres totals unaffected: {}", e.toString());
        }
        return true;
    }
}
