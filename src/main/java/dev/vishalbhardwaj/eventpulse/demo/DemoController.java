package dev.vishalbhardwaj.eventpulse.demo;

import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import dev.vishalbhardwaj.eventpulse.metrics.LagReporter;
import dev.vishalbhardwaj.eventpulse.metrics.PipelineStats;
import dev.vishalbhardwaj.eventpulse.producer.OrderSimulator;
import dev.vishalbhardwaj.eventpulse.repo.DlqMessageRepository;
import dev.vishalbhardwaj.eventpulse.repo.RevenueMinuteRepository;

/**
 * Chaos + proof endpoints that drive the live demo (see docs/demo-script-60s.md).
 */
@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final OrderSimulator simulator;
    private final PipelineStats stats;
    private final LagReporter lagReporter;
    private final RevenueMinuteRepository revenueMinutes;
    private final DlqMessageRepository dlqMessages;

    public DemoController(OrderSimulator simulator,
                          PipelineStats stats,
                          LagReporter lagReporter,
                          RevenueMinuteRepository revenueMinutes,
                          DlqMessageRepository dlqMessages) {
        this.simulator = simulator;
        this.stats = stats;
        this.lagReporter = lagReporter;
        this.revenueMinutes = revenueMinutes;
        this.dlqMessages = dlqMessages;
    }

    /** Pipeline counters — used to prove zero loss across the broker kill. */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of(
                "produced", stats.produced(),
                "consumed", stats.consumed(),
                "consumedUnique", stats.consumedUnique(),
                "revenueTotalCents", revenueMinutes.totalCents(),
                "revenueOrderCount", revenueMinutes.totalOrders(),
                "dlqPending", dlqMessages.countByReplayedFalse(),
                "consumerLagTotal", lagReporter.totalLag(),
                "simulatorSchemaVersion", simulator.getSchemaVersion());
    }

    /** Inject a poison (non-Avro) record → watch it land in the DLQ. */
    @PostMapping("/poison")
    public Map<String, Object> poison() {
        simulator.producePoison();
        return Map.of("status", "poison record produced, check GET /api/dlq/pending");
    }

    /**
     * Send the same business order {@code count} times.
     * Revenue must move exactly once — the idempotency proof.
     */
    @PostMapping("/duplicate")
    public Map<String, Object> duplicate(
            @RequestParam(defaultValue = "5") int count) {
        String orderId = "dup-" + UUID.randomUUID();
        long before = revenueMinutes.totalOrders();
        simulator.produceDuplicate(orderId, count);
        return Map.of(
                "orderId", orderId,
                "copiesProduced", count,
                "revenueOrderCountBefore", before,
                "note", "poll GET /api/demo/stats until consumed catches up; revenueOrderCount must increase by exactly 1");
    }

    /** Flip the simulator to schema v2 mid-stream — the evolution demo. */
    @PostMapping("/schema-version/{version}")
    public Map<String, Object> schemaVersion(@PathVariable String version) {
        simulator.setSchemaVersion(version);
        return Map.of("simulatorSchemaVersion", simulator.getSchemaVersion());
    }
}
