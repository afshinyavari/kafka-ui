package se.afshin.yavari.kafka.ui.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.security.Mode;

class KafkaUiConfigTest {
    private static KafkaUiConfig load(Map<String, String> extra) {
        var props = new java.util.HashMap<>(Map.of(
                "kafka-ui.mode", "mtls",
                "kafka-ui.bootstrap-servers", "k:9093",
                "kafka-ui.tls.ca-file", "/certs/ca.crt",
                "kafka-ui.registry.url", "http://reg/apis/registry/v3"));
        props.putAll(extra);
        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .withConverter(Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
                .withMapping(KafkaUiConfig.class)
                .withSources(new io.smallrye.config.PropertiesConfigSource(props, "test", 100))
                .build();
        return config.getConfigMapping(KafkaUiConfig.class);
    }

    @Test
    void searchDefaults() {
        var c = load(Map.of());
        assertEquals(Mode.MTLS, c.mode());
        assertEquals(100000, c.search().maxScanned());
        assertEquals(500, c.search().maxMatches());
        assertEquals(Duration.ofMillis(100), c.search().regexTimeout());
        assertEquals(Duration.ofHours(1), c.search().defaultRange());
        assertEquals(1000, c.tail().maxRows());
    }

    @Test
    void produceDefaultsAndTls() {
        var c = load(Map.of());
        assertEquals("apicurio.value.", c.produce().valueHeaderPrefix());
        assertEquals("apicurio.key.", c.produce().keyHeaderPrefix());
        assertTrue(c.tls().certFile().isEmpty());
        assertEquals(Path.of("/certs/ca.crt"), c.tls().caFile());
    }

    @Test
    void modeIsParsedCaseInsensitively() {
        assertEquals(Mode.OIDC, load(Map.of("kafka-ui.mode", "OIDC")).mode());
    }
}
