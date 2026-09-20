package dev.vishalbhardwaj.eventpulse.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import dev.vishalbhardwaj.eventpulse.avro.OrderEvent;
import dev.vishalbhardwaj.eventpulse.metrics.LatencyTracker;
import dev.vishalbhardwaj.eventpulse.metrics.PipelineStats;

/**
 * The money path: every order record → idempotent aggregate write → manual ack.
 *
 * <p>Poison records never reach here — deserialization failures are routed to the
 * DLQ by the container error handler (see {@code KafkaConfig}).
 */
@Component
public class OrderConsumer {

    private final RevenueService revenueService;
    private final LatencyTracker latencyTracker;
    private final PipelineStats stats;

    public OrderConsumer(RevenueService revenueService,
                         LatencyTracker latencyTracker,
                         PipelineStats stats) {
        this.revenueService = revenueService;
        this.latencyTracker = latencyTracker;
        this.stats = stats;
    }

    @KafkaListener(id = "order-consumer", topics = "${eventpulse.topics.orders}")
    public void listen(ConsumerRecord<String, OrderEvent> record,
                       Acknowledgment ack) {
        stats.incrementConsumed();

        OrderEvent event = record.value();
        if (event != null) {
            boolean applied = revenueService.applyIfNew(
                    record.topic(), record.partition(), record.offset(), event);
            if (applied) {
                stats.incrementConsumedUnique();
                latencyTracker.record(event.getProducedAt());
            }
        }
        // Ack ONLY after the DB write succeeded — a crash before this line
        // redelivers, and the dedupe table makes redelivery a no-op.
        ack.acknowledge();
    }
}
