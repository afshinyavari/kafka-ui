package se.afshin.yavari.kafka.ui.produce;

import java.util.HashMap;
import java.util.Map;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;

/** Dev Services Kafka plus the registry URL pointed at the WireMock server of {@link ProduceResourceTest}. */
public class ProduceProfile extends DevKafkaProfile {

    static final int REGISTRY_PORT = 18089;

    @Override
    public Map<String, String> getConfigOverrides() {
        var m = new HashMap<>(super.getConfigOverrides());
        m.put("kafka-ui.registry.url", "http://localhost:" + REGISTRY_PORT + "/apis/registry/v3");
        return m;
    }
}
