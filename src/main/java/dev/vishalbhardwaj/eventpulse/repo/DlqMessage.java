package dev.vishalbhardwaj.eventpulse.repo;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Quarantined poison messages, persisted by the DLQ consumer for inspection
 * and replay via {@code DlqController}.
 */
@Entity
@Table(name = "dlq_messages")
public class DlqMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String sourceTopic;
    private int sourcePartition;
    private long sourceOffset;
    private String keyHint;

    @Column(length = 2000)
    private String exceptionMessage;

    /** Original raw payload, base64 — replayable byte-for-byte. */
    @Column(columnDefinition = "TEXT")
    private String rawBase64;

    private boolean replayed;
    private Instant receivedAt;

    protected DlqMessage() {
    }

    public DlqMessage(String sourceTopic, int sourcePartition, long sourceOffset,
                      String keyHint, String exceptionMessage, String rawBase64) {
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.keyHint = keyHint;
        this.exceptionMessage = exceptionMessage;
        this.rawBase64 = rawBase64;
        this.replayed = false;
        this.receivedAt = Instant.now();
    }

    public Long getId() { return id; }
    public String getSourceTopic() { return sourceTopic; }
    public int getSourcePartition() { return sourcePartition; }
    public long getSourceOffset() { return sourceOffset; }
    public String getKeyHint() { return keyHint; }
    public String getExceptionMessage() { return exceptionMessage; }
    public String getRawBase64() { return rawBase64; }
    public boolean isReplayed() { return replayed; }
    public Instant getReceivedAt() { return receivedAt; }
    public void markReplayed() { this.replayed = true; }
}
