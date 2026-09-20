package dev.vishalbhardwaj.eventpulse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafka
@EnableScheduling
@ConfigurationPropertiesScan
public class EventPulseApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventPulseApplication.class, args);
    }
}
