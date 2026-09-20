package dev.vishalbhardwaj.eventpulse.repo;

import java.time.LocalDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RevenueMinuteRepository extends JpaRepository<RevenueMinute, LocalDateTime> {

    @Modifying
    @Query(value = """
            INSERT INTO revenue_minutes(minute, order_count, total_cents)
            VALUES (:minute, 1, :cents)
            ON CONFLICT (minute) DO UPDATE SET
              order_count = revenue_minutes.order_count + 1,
              total_cents = revenue_minutes.total_cents + EXCLUDED.total_cents
            """, nativeQuery = true)
    void upsert(@Param("minute") LocalDateTime minute, @Param("cents") long cents);

    @Query(value = "SELECT COALESCE(SUM(total_cents), 0) FROM revenue_minutes", nativeQuery = true)
    long totalCents();

    @Query(value = "SELECT COALESCE(SUM(order_count), 0) FROM revenue_minutes", nativeQuery = true)
    long totalOrders();
}
