package dev.vishalbhardwaj.eventpulse.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import dev.vishalbhardwaj.eventpulse.config.EventPulseProperties;
import dev.vishalbhardwaj.eventpulse.metrics.LagReporter;
import dev.vishalbhardwaj.eventpulse.metrics.LatencyTracker;
import dev.vishalbhardwaj.eventpulse.metrics.PipelineStats;
import dev.vishalbhardwaj.eventpulse.producer.OrderSimulator;

/**
 * One-shot throughput/latency measurement.
 *
 * <p>Run: {@code java -jar eventpulse-*.jar --spring.profiles.active=load
 * --eventpulse.load.count=600000 --eventpulse.load.rate-per-second=10000}
 *
 * <p>Produces {@code count} events at the target rate, waits for the consumer to
 * drain (lag back to zero), prints a report with achieved throughput and
 * end-to-end p50/p95/p99, then exits. The steady simulator is disabled under
 * the {@code load} profile so the numbers are clean.
 */
@Component
@Profile("load")
public class LoadRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadRunner.class);

    private final EventPulseProperties props;
    private final OrderSimulator simulator;
    private final PipelineStats stats;
    private final LatencyTracker latencyTracker;
    private final LagReporter lagReporter;
    private final ApplicationContext context;

    public LoadRunner(EventPulseProperties props,
                      OrderSimulator simulator,
                      PipelineStats stats,
                      LatencyTracker latencyTracker,
                      LagReporter lagReporter,
                      ApplicationContext context) {
        this.props = props;
        this.simulator = simulator;
        this.stats = stats;
        this.latencyTracker = latencyTracker;
        this.lagReporter = lagReporter;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        long count = props.load().count();
        int rate = props.load().ratePerSecond();
        long drainTimeoutMs = props.load().drainTimeoutSeconds() * 1000;

        log.info("=== LOAD TEST starting: {} events at {}/sec ===", count, rate);
        latencyTracker.reset();
        long start = System.nanoTime();
        simulator.burst(count, rate);
        long produceMs = (System.nanoTime() - start) / 1_000_000;
        log.info("Produced {} events in {} ms", count, produceMs);

        // Wait for drain: lag back to zero and all events consumed.
        long deadline = System.currentTimeMillis() + drainTimeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (lagReporter.totalLag() == 0 && stats.consumed() >= stats.produced()) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        long totalMs = (System.nanoTime() - start) / 1_000_000;
        double achievedPerSec = count / (totalMs / 1000.0);
        LatencyTracker.Snapshot snap = latencyTracker.snapshot();

        System.out.println();
        System.out.println("=== EVENTPULSE LOAD TEST REPORT ===");
        System.out.printf("events produced : %,d%n", stats.produced());
        System.out.printf("events consumed : %,d (unique business orders: %,d)%n",
                stats.consumed(), stats.consumedUnique());
        System.out.printf("target rate     : %,d/sec%n", rate);
        System.out.printf("achieved rate   : %,.0f/sec (produce+drain)%n", achievedPerSec);
        System.out.printf("e2e latency     : min=%dms p50=%dms p95=%dms p99=%dms max=%dms (n=%,d)%n",
                snap.minMs(), snap.p50Ms(), snap.p95Ms(), snap.p99Ms(), snap.maxMs(), snap.count());
        long lost = stats.produced() - stats.consumed();
        System.out.printf("lost messages   : %,d %s%n", lost, lost == 0 ? "(ZERO LOSS)" : "(!!! INVESTIGATE)");
        System.out.println("===================================");
        System.out.println();
        // Machine-parseable line for CI.
        System.out.printf("LOAD_RESULT produced=%d consumed=%d unique=%d lost=%d rate=%d achieved=%.0f p50=%d p95=%d p99=%d max=%d%n",
                stats.produced(), stats.consumed(), stats.consumedUnique(), lost, rate, achievedPerSec,
                snap.p50Ms(), snap.p95Ms(), snap.p99Ms(), snap.maxMs());

        int exitCode = SpringApplication.exit(context, () -> lost == 0 ? 0 : 1);
        System.exit(exitCode);
    }
}
