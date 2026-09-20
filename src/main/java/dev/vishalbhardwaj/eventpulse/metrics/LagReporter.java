package dev.vishalbhardwaj.eventpulse.metrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.vishalbhardwaj.eventpulse.config.EventPulseProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import jakarta.annotation.PreDestroy;

/**
 * Per-partition consumer lag as Micrometer gauges ({@code eventpulse.consumer.lag}),
 * scraped by Prometheus and shown in Grafana with an alert threshold.
 *
 * <p>Lag is computed the way production tooling does it:
 * {@code log-end-offset − committed-offset}, via {@link AdminClient} (thread-safe)
 * and a dedicated probe consumer used only from the scheduler thread. We
 * deliberately do NOT touch the listener's consumer — {@code KafkaConsumer} is
 * not thread-safe and sampling it from another thread corrupts the poll loop.
 *
 * <p>When lag crosses the configured threshold we log loudly and bump an alert
 * counter — in production this is where a PagerDuty/OpsGenie webhook goes
 * (see docs/runbook.md).
 */
@Component
public class LagReporter {

    private static final Logger log = LoggerFactory.getLogger(LagReporter.class);

    private final MeterRegistry registry;
    private final AdminClient admin;
    private final KafkaConsumer<String, Object> probe;
    private final String groupId;
    private final String topic;
    private final long alertThreshold;
    private final Counter alertCounter;
    private final Map<TopicPartition, AtomicLong> gauges = new ConcurrentHashMap<>();
    private volatile List<TopicPartition> partitions = List.of();

    public LagReporter(MeterRegistry registry,
                       KafkaProperties kafkaProperties,
                       EventPulseProperties props) {
        this.registry = registry;
        this.groupId = kafkaProperties.getConsumer().getGroupId();
        this.topic = props.topics().orders();
        this.alertThreshold = props.consumer().lagAlertThreshold();
        this.alertCounter = Counter.builder("eventpulse.consumer.lag.alerts")
                .description("Times per-partition lag crossed the alert threshold")
                .register(registry);

        Map<String, Object> adminProps = Map.of(
                "bootstrap.servers", String.join(",", kafkaProperties.getBootstrapServers()));
        this.admin = AdminClient.create(adminProps);

        Map<String, Object> probeProps = new java.util.HashMap<>();
        probeProps.put("bootstrap.servers", String.join(",", kafkaProperties.getBootstrapServers()));
        probeProps.put("group.id", "eventpulse-lag-probe");
        probeProps.put("key.deserializer", StringDeserializer.class);
        probeProps.put("value.deserializer", ByteArrayDeserializer.class);
        this.probe = new KafkaConsumer<>(probeProps);
        // Partitions are resolved lazily on the first scheduled tick so a
        // not-yet-ready Kafka doesn't stall application startup.
    }

    @PreDestroy
    public void close() {
        try { probe.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
        try { admin.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
    }

    @Scheduled(fixedDelay = 5000)
    public void report() {
        try {
            if (partitions.isEmpty()) {
                refreshPartitions();
                if (partitions.isEmpty()) {
                    return;
                }
            }
            Set<TopicPartition> tps = ConcurrentHashMap.newKeySet();
            tps.addAll(partitions);

            Map<TopicPartition, Long> endOffsets = probe.endOffsets(tps);
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(Duration.ofSeconds(10));

            for (TopicPartition tp : tps) {
                Long end = endOffsets.get(tp);
                if (end == null) {
                    continue;
                }
                OffsetAndMetadata om = committed.get(tp);
                long lag = Math.max(0, end - (om != null ? om.offset() : 0));
                gauge(tp).set(lag);
                if (lag >= alertThreshold) {
                    alertCounter.increment();
                    log.warn("LAG ALERT topic={} partition={} lag={} (threshold={})",
                            tp.topic(), tp.partition(), lag, alertThreshold);
                }
            }
        } catch (Exception e) {
            log.debug("Lag sampling failed: {}", e.toString());
        }
    }

    /** Total lag across partitions — used by the load test's drain wait. */
    public long totalLag() {
        return gauges.values().stream().mapToLong(AtomicLong::get).sum();
    }

    private void refreshPartitions() {
        try {
            List<TopicPartition> fresh = new ArrayList<>();
            probe.partitionsFor(topic).forEach(pi ->
                    fresh.add(new TopicPartition(topic, pi.partition())));
            partitions = fresh;
        } catch (Exception e) {
            log.debug("Could not list partitions for {}: {}", topic, e.toString());
        }
    }

    private AtomicLong gauge(TopicPartition tp) {
        return gauges.computeIfAbsent(tp, key -> {
            AtomicLong value = new AtomicLong(0);
            registry.gauge("eventpulse.consumer.lag", List.of(
                            Tag.of("topic", key.topic()),
                            Tag.of("partition", String.valueOf(key.partition()))),
                    value, AtomicLong::get);
            return value;
        });
    }
}
