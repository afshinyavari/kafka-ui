package se.afshin.yavari.kafka.ui.schemas;

import java.util.HashMap;
import java.util.Map;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;

/** Dev Services Kafka plus the registry URL pointed at the WireMock server of {@link SchemasResourceTest}. */
public class SchemasProfile extends DevKafkaProfile {

    static final int REGISTRY_PORT = 18090;

    @Override
    public Map<String, String> getConfigOverrides() {
        var m = new HashMap<>(super.getConfigOverrides());
        m.put("kafka-ui.registry.url", "http://localhost:" + REGISTRY_PORT + "/apis/registry/v3");
        return m;
    }
}
