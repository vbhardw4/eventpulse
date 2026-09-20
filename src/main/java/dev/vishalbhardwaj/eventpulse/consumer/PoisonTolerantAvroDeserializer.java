package dev.vishalbhardwaj.eventpulse.consumer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Avro value deserializer that converts poison into data instead of an
 * exception.
 *
 * <p>A raw {@code KafkaAvroDeserializer} throws on malformed bytes, and in
 * batch-listener mode that exception aborts the whole poll batch — one poison
 * record would wedge the partition. This wrapper catches the failure and
 * returns a {@link PoisonRecord} carrying the original bytes, so the listener
 * can quarantine it to the DLQ and keep processing the rest of the batch.
 * All configuration (schema.registry.url, specific.avro.reader, …) is passed
 * straight through to the delegate.
 */
public class PoisonTolerantAvroDeserializer implements Deserializer<Object> {

    private final KafkaAvroDeserializer delegate = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return delegate.deserialize(topic, data);
        } catch (Exception e) {
            return new PoisonRecord(data, e.toString());
        }
    }

    @Override
    public void close() {
        delegate.close();
    }
}
