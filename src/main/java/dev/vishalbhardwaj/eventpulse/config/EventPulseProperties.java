package dev.vishalbhardwaj.eventpulse.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tuning knobs for the pipeline, bound from {@code eventpulse.*} in application.yml.
 */
@ConfigurationProperties(prefix = "eventpulse")
public record EventPulseProperties(
        Topics topics,
        Simulator simulator,
        Consumer consumer,
        Load load) {

    public record Topics(
            String orders,
            String dlq,
            int ordersPartitions,
            int dlqPartitions,
            short replicationFactor,
            short minInsyncReplicas) {}

    public record Simulator(
            boolean enabled,
            int ratePerSecond,
            String schemaVersion) {}

    public record Consumer(
            long lagAlertThreshold) {}

    public record Load(
            boolean enabled,
            long count,
            int ratePerSecond,
            long drainTimeoutSeconds) {}
}
