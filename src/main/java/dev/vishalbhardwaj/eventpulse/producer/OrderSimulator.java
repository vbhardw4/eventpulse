package dev.vishalbhardwaj.eventpulse.producer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.vishalbhardwaj.eventpulse.avro.OrderEvent;
import dev.vishalbhardwaj.eventpulse.config.EventPulseProperties;
import dev.vishalbhardwaj.eventpulse.metrics.PipelineStats;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Simulates a storefront emitting order events.
 *
 * <p>Schema modes: {@code v1} builds {@link GenericRecord}s from the original
 * v1 schema; {@code v2} builds the generated {@link OrderEvent} (adds
 * {@code promoCode}). Flipping modes mid-stream registers v2 on the
 * {@code orders-value} subject — the schema-evolution demo. The consumer's v2
 * reader keeps reading v1 records (BACKWARD compatibility).
 */
@Component
public class OrderSimulator {

    private static final Logger log = LoggerFactory.getLogger(OrderSimulator.class);

    private final KafkaTemplate<String, Object> orderTemplate;
    private final KafkaTemplate<String, byte[]> rawTemplate;
    private final EventPulseProperties props;
    private final PipelineStats stats;
    private final AtomicReference<String> schemaVersion;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile Schema v1Schema;

    public OrderSimulator(KafkaTemplate<String, Object> orderTemplate,
                          KafkaTemplate<String, byte[]> rawTemplate,
                          EventPulseProperties props,
                          PipelineStats stats) {
        this.orderTemplate = orderTemplate;
        this.rawTemplate = rawTemplate;
        this.props = props;
        this.stats = stats;
        this.schemaVersion = new AtomicReference<>(props.simulator().schemaVersion());
    }

    @PostConstruct
    public void init() {
        try (InputStream in = getClass().getResourceAsStream("/avro/order-event-v1.avsc")) {
            if (in == null) {
                throw new IllegalStateException("order-event-v1.avsc not on classpath");
            }
            v1Schema = new Schema.Parser().parse(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse v1 schema", e);
        }
        if (props.simulator().enabled()) {
            int rate = props.simulator().ratePerSecond();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    produceBatch(rate);
                } catch (Exception e) {
                    log.warn("Simulator batch failed: {}", e.toString());
                }
            }, 2, 1, TimeUnit.SECONDS);
            log.info("Order simulator started: {} orders/sec, schema {}", rate, schemaVersion.get());
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }

    public String getSchemaVersion() {
        return schemaVersion.get();
    }

    public void setSchemaVersion(String version) {
        if (!version.equals("v1") && !version.equals("v2")) {
            throw new IllegalArgumentException("schema version must be v1 or v2");
        }
        schemaVersion.set(version);
        log.info("Simulator schema version switched to {}", version);
    }

    /** Produces {@code count} orders atomically in one Kafka transaction. */
    public void produceBatch(int count) {
        List<Object> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            events.add(buildEvent(UUID.randomUUID().toString()));
        }
        orderTemplate.executeInTransaction(ops -> {
            for (Object event : events) {
                ops.send(props.topics().orders(), orderIdOf(event), event);
            }
            return null;
        });
        stats.incrementProduced(count);
    }

    /** High-rate burst with pacing, used by the load test. */
    public void burst(long total, int ratePerSecond) {
        int chunk = Math.max(1, ratePerSecond / 10);
        long sent = 0;
        long nextTick = System.nanoTime();
        while (sent < total) {
            int n = (int) Math.min(chunk, total - sent);
            produceBatch(n);
            sent += n;
            nextTick += 100_000_000L;
            long sleepNanos = nextTick - System.nanoTime();
            if (sleepNanos > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(sleepNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                nextTick = System.nanoTime(); // fell behind; don't compound the debt
            }
        }
    }

    /** Sends the same business order {@code count} times — the idempotency demo. */
    public void produceDuplicate(String orderId, int count) {
        Object event = buildEvent(orderId);
        orderTemplate.executeInTransaction(ops -> {
            for (int i = 0; i < count; i++) {
                ops.send(props.topics().orders(), orderId, event);
            }
            return null;
        });
        stats.incrementProduced(count);
        log.info("Produced {} copies of order {}", count, orderId);
    }

    /** Sends bytes that are not Avro at all — triggers the DLQ path. */
    public void producePoison() {
        rawTemplate.send(props.topics().orders(), "poison-key",
                "THIS-IS-NOT-AVRO".getBytes(StandardCharsets.UTF_8));
        stats.incrementProduced();
        log.info("Produced poison record to {}", props.topics().orders());
    }

    // ------------------------------------------------------------------

    private Object buildEvent(String orderId) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();
        if ("v2".equals(schemaVersion.get())) {
            return OrderEvent.newBuilder()
                    .setOrderId(orderId)
                    .setCustomerId("cust-" + rnd.nextInt(1, 5000))
                    .setAmountCents(500L + rnd.nextLong(49_500L))
                    .setCurrency("USD")
                    .setItemCount(1 + rnd.nextInt(5))
                    .setProducedAt(now)
                    .setSchemaVersion("v2")
                    .setPromoCode(rnd.nextBoolean() ? "SAVE10" : null)
                    .build();
        }
        GenericRecord record = new GenericData.Record(v1Schema);
        record.put("orderId", orderId);
        record.put("customerId", "cust-" + rnd.nextInt(1, 5000));
        record.put("amountCents", 500L + rnd.nextLong(49_500L));
        record.put("currency", "USD");
        record.put("itemCount", 1 + rnd.nextInt(5));
        record.put("producedAt", now);
        record.put("schemaVersion", "v1");
        return record;
    }

    private static String orderIdOf(Object event) {
        if (event instanceof OrderEvent e) {
            return e.getOrderId().toString();
        }
        return ((GenericRecord) event).get("orderId").toString();
    }
}
