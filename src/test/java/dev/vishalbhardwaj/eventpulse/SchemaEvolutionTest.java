package dev.vishalbhardwaj.eventpulse;

import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.Test;

import dev.vishalbhardwaj.eventpulse.avro.OrderEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Guards the schema-evolution demo: v1 records must remain readable by the v2
 * reader (the consumer always reads with the v2 schema).
 */
class SchemaEvolutionTest {

    private static Schema v1Schema() throws Exception {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream("/avro/order-event-v1.avsc")) {
            assertNotNull(in, "v1 schema missing from test classpath");
            return new Schema.Parser().parse(in);
        }
    }

    @Test
    void v2ReaderCanReadV1Writer() throws Exception {
        Schema writer = v1Schema();
        Schema reader = OrderEvent.getClassSchema();

        SchemaCompatibility.SchemaPairCompatibility result =
                SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);

        assertEquals(SchemaCompatibility.SchemaCompatibilityResult.COMPATIBLE,
                result.getResult(),
                "v2 reader must stay BACKWARD compatible with v1, got: " + result.getResult());
    }

    @Test
    void v2PromoCodeDefaultsToNull() {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId("o-1")
                .setCustomerId("c-1")
                .setAmountCents(1000L)
                .setCurrency("USD")
                .setItemCount(1)
                .setProducedAt(System.currentTimeMillis())
                .setSchemaVersion("v2")
                .build();
        assertNull(event.getPromoCode(), "promoCode must default to null for v1-origin records");
    }
}
