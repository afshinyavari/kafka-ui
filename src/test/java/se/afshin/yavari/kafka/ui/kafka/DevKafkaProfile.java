package se.afshin.yavari.kafka.ui.kafka;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Set;
import java.util.Map;

/** mTLS mode against a Dev Services Kafka broker, reached through {@link PlaintextCustomizer}. */
public class DevKafkaProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("kafka-ui.mode", "mtls", "quarkus.kafka.devservices.enabled", "true");
    }

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
        return Set.of(PlaintextCustomizer.class);
    }
}
