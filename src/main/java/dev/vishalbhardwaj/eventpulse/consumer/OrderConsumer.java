package dev.vishalbhardwaj.eventpulse.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import dev.vishalbhardwaj.eventpulse.avro.OrderEvent;
import dev.vishalbhardwaj.eventpulse.consumer.RevenueService.PendingOrder;
import dev.vishalbhardwaj.eventpulse.metrics.LatencyTracker;
import dev.vishalbhardwaj.eventpulse.metrics.PipelineStats;

/**
 * The money path, in batches: every poll batch → one idempotent DB batch →
 * one manual ack.
 *
 * <p>Poison records never wedge the batch — the value deserializer converts
 * them to {@link PoisonRecord} (raw bytes preserved) and they go straight to
 * the DLQ topic, bypassing retries entirely. Deserialization can no longer
 * fail the poll.
 */
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final RevenueService revenueService;
    private final DeadLetterPublishingRecoverer dlqRecoverer;
    private final LatencyTracker latencyTracker;
    private final PipelineStats stats;

    public OrderConsumer(RevenueService revenueService,
                         DeadLetterPublishingRecoverer dlqRecoverer,
                         LatencyTracker latencyTracker,
                         PipelineStats stats) {
        this.revenueService = revenueService;
        this.dlqRecoverer = dlqRecoverer;
        this.latencyTracker = latencyTracker;
        this.stats = stats;
    }

    @KafkaListener(id = "order-consumer", topics = "${eventpulse.topics.orders}")
    public void listen(List<ConsumerRecord<String, Object>> records,
                       Acknowledgment ack) {
        stats.incrementConsumed(records.size());

        List<PendingOrder> pending = new ArrayList<>(records.size());
        for (var record : records) {
            Object value = record.value();
            if (value instanceof PoisonRecord poison) {
                // Never retried: quarantine with the original bytes and move on.
                dlqRecoverer.accept(
                        new ConsumerRecord<>(record.topic(), record.partition(),
                                record.offset(), record.key(), poison.rawBytes()),
                        new SerializationException("Avro deserialization failed: " + poison.error()));
            } else if (value instanceof OrderEvent event) {
                pending.add(new PendingOrder(
                        record.topic(), record.partition(), record.offset(), event));
            } else if (value != null) {
                log.warn("Unexpected record value type {}, skipping (offset {})",
                        value.getClass().getName(), record.offset());
            }
            // null value without a poison marker: tombstone — skip silently.
        }

        List<OrderEvent> applied = revenueService.applyBatch(pending);
        stats.incrementConsumedUnique(applied.size());
        for (OrderEvent event : applied) {
            latencyTracker.record(event.getProducedAt());
        }
        // Ack ONLY after the DB batch committed — a crash before this line
        // redelivers, and the dedupe table makes redelivery a no-op.
        ack.acknowledge();
    }
}
