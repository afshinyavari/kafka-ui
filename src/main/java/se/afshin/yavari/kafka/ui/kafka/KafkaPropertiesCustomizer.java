package se.afshin.yavari.kafka.ui.kafka;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.Properties;

/** Last-step hook on every client's properties. Tests replace it to reach a plaintext broker. */
public interface KafkaPropertiesCustomizer {

    Properties customize(Properties properties);

    @ApplicationScoped
    class Config {
        @Produces
        @DefaultBean
        @ApplicationScoped
        KafkaPropertiesCustomizer identity() {
            return properties -> properties;
        }
    }
}
