package dev.vishalbhardwaj.eventpulse.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * End-to-end latency: {@code now - producedAt} per consumed order.
 * Exposed both as a Micrometer {@link Timer} (Prometheus histogram for Grafana)
 * and as an in-memory reservoir for the load-test's printed percentile report.
 */
@Component
public class LatencyTracker {

    private final Timer timer;
    private final List<Long> samples = new CopyOnWriteArrayList<>();

    public LatencyTracker(MeterRegistry registry) {
        this.timer = Timer.builder("eventpulse.e2e.latency")
                .description("End-to-end latency from produce to consume, milliseconds")
                .publishPercentileHistogram()
                .register(registry);
    }

    public void record(long producedAtEpochMillis) {
        long latencyMs = Math.max(0, System.currentTimeMillis() - producedAtEpochMillis);
        timer.record(latencyMs, TimeUnit.MILLISECONDS);
        samples.add(latencyMs);
    }

    public Snapshot snapshot() {
        List<Long> sorted = new ArrayList<>(samples);
        Collections.sort(sorted);
        if (sorted.isEmpty()) {
            return new Snapshot(0, 0, 0, 0, 0, 0);
        }
        return new Snapshot(
                sorted.size(),
                sorted.get(0),
                percentile(sorted, 50),
                percentile(sorted, 95),
                percentile(sorted, 99),
                sorted.get(sorted.size() - 1));
    }

    public void reset() {
        samples.clear();
    }

    private static long percentile(List<Long> sorted, int p) {
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    public record Snapshot(long count, long minMs, long p50Ms, long p95Ms, long p99Ms, long maxMs) {
    }
}
