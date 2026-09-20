package dev.vishalbhardwaj.eventpulse.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka wiring.
 *
 * <p>Exactly-once story, stated precisely (no theater):
 * <ul>
 *   <li>Producer is <b>transactional</b> ({@code transaction-id-prefix} in yml):
 *       idempotent + atomic multi-partition writes, and consumers with
 *       {@code isolation.level=read_committed} never see aborted messages.</li>
 *   <li>Consumer is <b>idempotent on the business key</b> ({@code orderId}):
 *       the DB write and the offset commit are NOT atomic, so a crash between
 *       them causes redelivery — the {@code processed_orders} dedupe table makes
 *       redelivery a no-op. This is the pattern most production Kafka-to-DB
 *       pipelines actually use (Kafka transactions alone only cover
 *       Kafka-to-Kafka).</li>
 *   <li>Offsets are committed manually, only after the DB write succeeds.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    private final EventPulseProperties props;

    public KafkaConfig(EventPulseProperties props) {
        this.props = props;
    }

    // ------------------------------------------------------------------
    // Topics
    // ------------------------------------------------------------------

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(props.topics().orders())
                .partitions(props.topics().ordersPartitions())
                .replicas(props.topics().replicationFactor())
                .configs(Map.of("min.insync.replicas",
                        String.valueOf(props.topics().minInsyncReplicas())))
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name(props.topics().dlq())
                .partitions(props.topics().dlqPartitions())
                .replicas(props.topics().replicationFactor())
                .configs(Map.of("min.insync.replicas",
                        String.valueOf(props.topics().minInsyncReplicas())))
                .build();
    }

    // ------------------------------------------------------------------
    // Producers
    // ------------------------------------------------------------------

    /** Transactional Avro producer for order events (Spring Boot binds the yml). */
    @Bean
    public ProducerFactory<String, Object> orderProducerFactory(KafkaProperties kafkaProperties) {
        var factory = new DefaultKafkaProducerFactory<String, Object>(
                kafkaProperties.buildProducerProperties(null));
        // buildProducerProperties() does NOT carry the transaction prefix into the
        // map — Boot normally applies it via setTransactionIdPrefix on the
        // auto-configured factory. We build our own factory, so apply it here.
        // Without this line the template silently runs NON-transactionally.
        String prefix = kafkaProperties.getProducer().getTransactionIdPrefix();
        if (prefix != null) {
            factory.setTransactionIdPrefix(prefix);
        }
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> orderKafkaTemplate(
            ProducerFactory<String, Object> orderProducerFactory) {
        return new KafkaTemplate<>(orderProducerFactory);
    }

    /** Plain byte[] producer for DLQ publishing and poison-message injection. */
    @Bean
    public KafkaTemplate<String, byte[]> rawKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> p = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        p.remove("transactional.id");
        p.remove("transaction.id.prefix");
        p.put("key.serializer", StringSerializer.class);
        p.put("value.serializer", ByteArraySerializer.class);
        p.put("enable.idempotence", true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(p));
    }

    // ------------------------------------------------------------------
    // Consumers
    // ------------------------------------------------------------------

    @Bean
    public ConsumerFactory<String, Object> orderConsumerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(null));
    }

    /**
     * Main listener factory: manual acks + DLQ routing for poison records.
     * Deserialization failures are NOT retried (they would fail forever) —
     * they go straight to {@code orders.dlq} with the original raw bytes.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> orderConsumerFactory,
            KafkaTemplate<String, byte[]> rawKafkaTemplate) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(orderConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);

        var recoverer = new DeadLetterPublishingRecoverer(rawKafkaTemplate,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(
                        props.topics().dlq(),
                        Math.abs(record.partition() % props.topics().dlqPartitions())));

        var errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 2L));
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                org.apache.kafka.common.errors.SerializationException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /** Byte[] factory for the DLQ inspector consumer (DLQ holds raw failed payloads). */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> dlqListenerContainerFactory(
            KafkaProperties kafkaProperties) {
        Map<String, Object> p = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        // The DLQ listener joins its own group — sharing the main group would
        // tangle partition assignment across two different topic subscriptions.
        p.put("group.id", "eventpulse-dlq");
        p.put("key.deserializer", StringDeserializer.class);
        p.put("value.deserializer", ByteArrayDeserializer.class);
        p.remove("spring.deserializer.value.delegate.class");
        p.remove("specific.avro.reader");
        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(p));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        return factory;
    }
}
