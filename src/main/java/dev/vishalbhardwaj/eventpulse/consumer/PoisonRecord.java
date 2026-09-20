package dev.vishalbhardwaj.eventpulse.consumer;

/**
 * A record whose bytes failed Avro deserialization.
 *
 * <p>Poison is never retried (it would fail forever and wedge the partition).
 * The batch listener routes it straight to the DLQ topic with the original raw
 * bytes preserved, so it can be inspected and replayed via {@code /api/dlq}.
 */
public record PoisonRecord(byte[] rawBytes, String error) {
}
