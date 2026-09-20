package dev.vishalbhardwaj.eventpulse.repo;

import java.time.Instant;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Idempotency keys. The business key ({@code orderId}) is the dedupe key — this
 * covers both transport redelivery (same offset after a crash, before the offset
 * commit) and business-level resubmission (same order sent twice).
 */
@Entity
@Table(name = "processed_orders")
public class ProcessedOrder {

    @Id
    private String orderId;

    private String topic;
    private int recordPartition;
    private long recordOffset;
    private Instant processedAt;

    protected ProcessedOrder() {
    }

    public ProcessedOrder(String orderId, String topic, int recordPartition, long recordOffset) {
        this.orderId = orderId;
        this.topic = topic;
        this.recordPartition = recordPartition;
        this.recordOffset = recordOffset;
        this.processedAt = Instant.now();
    }

    public String getOrderId() {
        return orderId;
    }
}
