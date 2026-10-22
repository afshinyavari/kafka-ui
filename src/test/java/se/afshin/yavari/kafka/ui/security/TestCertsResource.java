package se.afshin.yavari.kafka.ui.security;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.nio.file.Path;
import java.util.Map;

/** Generates the TLS files that src/test/resources/application.properties points at (target/test-certs). */
public class TestCertsResource implements QuarkusTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        TestCerts.generate(Path.of("target", "test-certs"));
        return Map.of();
    }

    @Override
    public void stop() {}
}
