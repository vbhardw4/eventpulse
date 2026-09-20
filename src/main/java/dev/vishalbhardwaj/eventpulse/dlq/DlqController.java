package dev.vishalbhardwaj.eventpulse.dlq;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import dev.vishalbhardwaj.eventpulse.config.EventPulseProperties;
import dev.vishalbhardwaj.eventpulse.repo.DlqMessage;
import dev.vishalbhardwaj.eventpulse.repo.DlqMessageRepository;

/**
 * Inspect and replay quarantined records.
 *
 * <p>Replay republishes the <b>original raw bytes</b> to the orders topic. If the
 * underlying problem is fixed (e.g. the producer bug that emitted garbage), the
 * record now deserializes and processes normally. If not, it lands back in the
 * DLQ — which is the correct, visible outcome, not silent loss.
 */
@RestController
@RequestMapping("/api/dlq")
public class DlqController {

    private final DlqMessageRepository dlqMessages;
    private final KafkaTemplate<String, byte[]> rawKafkaTemplate;
    private final EventPulseProperties props;

    public DlqController(DlqMessageRepository dlqMessages,
                         KafkaTemplate<String, byte[]> rawKafkaTemplate,
                         EventPulseProperties props) {
        this.dlqMessages = dlqMessages;
        this.rawKafkaTemplate = rawKafkaTemplate;
        this.props = props;
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return dlqMessages.findTop100ByOrderByReceivedAtDesc().stream()
                .map(this::toDto)
                .toList();
    }

    @GetMapping("/pending")
    public List<Map<String, Object>> pending() {
        return dlqMessages.findByReplayedFalseOrderByReceivedAtDesc().stream()
                .map(this::toDto)
                .toList();
    }

    @PostMapping("/replay")
    @Transactional
    public Map<String, Object> replayAll() {
        List<DlqMessage> pending = dlqMessages.findByReplayedFalseOrderByReceivedAtDesc();
        int replayed = 0;
        for (DlqMessage message : pending) {
            byte[] raw = java.util.Base64.getDecoder().decode(message.getRawBase64());
            // New key so a replayed poison record is distinguishable in the log;
            // the payload itself is byte-identical to the original.
            rawKafkaTemplate.send(props.topics().orders(),
                    "replay-" + message.getId() + "-" + UUID.randomUUID(),
                    raw);
            message.markReplayed();
            replayed++;
        }
        return Map.of("replayed", replayed);
    }

    private Map<String, Object> toDto(DlqMessage m) {
        return Map.of(
                "id", m.getId(),
                "sourceTopic", m.getSourceTopic(),
                "sourcePartition", m.getSourcePartition(),
                "sourceOffset", m.getSourceOffset(),
                "keyHint", m.getKeyHint() == null ? "" : m.getKeyHint(),
                "exception", m.getExceptionMessage(),
                "replayed", m.isReplayed(),
                "receivedAt", m.getReceivedAt().toString());
    }
}
