package dev.vishalbhardwaj.eventpulse.repo;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface DlqMessageRepository extends JpaRepository<DlqMessage, Long> {

    List<DlqMessage> findByReplayedFalseOrderByReceivedAtDesc();

    List<DlqMessage> findTop100ByOrderByReceivedAtDesc();

    long countByReplayedFalse();
}
