package dev.vishalbhardwaj.eventpulse.consumer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import dev.vishalbhardwaj.eventpulse.repo.DlqMessage;
import dev.vishalbhardwaj.eventpulse.repo.DlqMessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Persists quarantined records so they can be inspected and replayed via
 * {@code DlqController} instead of rotting in a topic nobody reads.
 */
@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    private final DlqMessageRepository dlqMessages;
    private final Counter dlqCounter;

    public DlqConsumer(DlqMessageRepository dlqMessages, MeterRegistry registry) {
        this.dlqMessages = dlqMessages;
        this.dlqCounter = Counter.builder("eventpulse.dlq.received")
                .description("Records quarantined to the dead-letter queue")
                .register(registry);
    }

    @KafkaListener(id = "dlq-consumer",
            topics = "${eventpulse.topics.dlq}",
            containerFactory = "dlqListenerContainerFactory")
    @Transactional("transactionManager")
    public void listen(ConsumerRecord<String, byte[]> record) {
        String exceptionMessage = headerAsString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        if (exceptionMessage == null) {
            exceptionMessage = headerAsString(record, KafkaHeaders.DLT_EXCEPTION_STACKTRACE);
        }
        String originalTopic = headerAsString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        byte[] raw = record.value();
        DlqMessage message = new DlqMessage(
                originalTopic != null ? originalTopic : record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                exceptionMessage != null ? truncate(exceptionMessage, 2000) : "unknown",
                raw != null ? Base64.getEncoder().encodeToString(raw) : "");
        dlqMessages.save(message);
        dlqCounter.increment();
        log.warn("DLQ quarantined record from {} (key={}): {}",
                message.getSourceTopic(), record.key(), message.getExceptionMessage());
    }

    private static String headerAsString(ConsumerRecord<String, byte[]> record, String header) {
        var h = record.headers().lastHeader(header);
        return h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }
}
