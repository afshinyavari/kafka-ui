package se.afshin.yavari.kafka.ui.web;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class MtlsProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("kafka-ui.mode", "mtls");
    }
}
