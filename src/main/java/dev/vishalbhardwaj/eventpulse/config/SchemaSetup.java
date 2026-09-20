package dev.vishalbhardwaj.eventpulse.config;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * Pins the {@code orders-value} subject to BACKWARD compatibility at startup so
 * the v1→v2 evolution demo is enforced by the registry, not by convention.
 * Retries briefly — Schema Registry can lag behind the brokers on first boot.
 */
@Component
@Order(1)
public class SchemaSetup implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SchemaSetup.class);

    private final String registryUrl;

    public SchemaSetup(org.springframework.core.env.Environment env) {
        this.registryUrl = env.getProperty("spring.kafka.properties.schema.registry.url",
                "http://localhost:8081");
    }

    @Override
    public void run(ApplicationArguments args) {
        RestTemplate rest = new RestTemplate();
        String url = registryUrl + "/config/orders-value";
        for (int attempt = 1; attempt <= 12; attempt++) {
            try {
                rest.put(url, Map.of("compatibility", "BACKWARD"));
                log.info("Schema Registry: orders-value compatibility set to BACKWARD");
                return;
            } catch (Exception e) {
                log.info("Schema Registry not ready (attempt {}/12): {}", attempt, e.getMessage());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        log.warn("Could not set BACKWARD compatibility on orders-value; "
                + "evolution demo guard not enforced — set it manually: "
                + "curl -X PUT {}/config/orders-value -H 'Content-Type: application/json' "
                + "-d '{{\"compatibility\":\"BACKWARD\"}}'", registryUrl);
    }
}
