package dev.vishalbhardwaj.eventpulse.repo;

import java.time.LocalDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, String> {

    /**
     * Idempotent insert: returns 1 if this orderId is seen for the first time,
     * 0 if it was already processed (redelivery / resubmission).
     */
    @Modifying
    @Query(value = """
            INSERT INTO processed_orders(order_id, topic, record_partition, record_offset, processed_at)
            VALUES (:orderId, :topic, :partition, :recordOffset, now())
            ON CONFLICT (order_id) DO NOTHING
            """, nativeQuery = true)
    int insertIgnore(@Param("orderId") String orderId,
                     @Param("topic") String topic,
                     @Param("partition") int partition,
                     @Param("recordOffset") long recordOffset);
}
