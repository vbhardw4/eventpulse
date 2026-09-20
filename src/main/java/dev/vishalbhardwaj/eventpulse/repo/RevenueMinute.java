package dev.vishalbhardwaj.eventpulse.repo;

import java.time.LocalDateTime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Per-minute revenue aggregates written by the stream consumer.
 * Grafana reads this table for the real-time revenue dashboard.
 */
@Entity
@Table(name = "revenue_minutes")
public class RevenueMinute {

    @Id
    private LocalDateTime minute;

    private long orderCount;
    private long totalCents;

    protected RevenueMinute() {
    }

    public LocalDateTime getMinute() {
        return minute;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public long getTotalCents() {
        return totalCents;
    }
}
