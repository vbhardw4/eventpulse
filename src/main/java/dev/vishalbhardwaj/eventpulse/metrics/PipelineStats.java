package dev.vishalbhardwaj.eventpulse.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Pipeline-wide counters for the demo API and the broker-kill verification. */
@Component
public class PipelineStats {

    private final AtomicLong produced = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong consumedUnique = new AtomicLong();
    private final Counter consumedCounter;

    public PipelineStats(MeterRegistry registry) {
        this.consumedCounter = Counter.builder("eventpulse.orders.consumed")
                .description("Order records consumed (including redeliveries)")
                .register(registry);
    }

    public void incrementProduced() {
        produced.incrementAndGet();
    }

    public void incrementProduced(long n) {
        produced.addAndGet(n);
    }

    public void incrementConsumed() {
        incrementConsumed(1);
    }

    public void incrementConsumed(long n) {
        consumed.addAndGet(n);
        consumedCounter.increment(n);
    }

    public void incrementConsumedUnique() {
        incrementConsumedUnique(1);
    }

    public void incrementConsumedUnique(long n) {
        consumedUnique.addAndGet(n);
    }

    public long produced() { return produced.get(); }
    public long consumed() { return consumed.get(); }
    public long consumedUnique() { return consumedUnique.get(); }
}
