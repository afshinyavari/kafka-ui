package se.afshin.yavari.kafka.ui.it;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real Apicurio Registry 3 (in-memory storage, no auth). Points {@code kafka-ui.registry.url} at it; the UI talks to
 * it over plain HTTP, so the TLS material from the test certificates is loaded but unused.
 */
public class ApicurioResource implements QuarkusTestResourceLifecycleManager {

    /** Keep in step with {@code apicurio.version} in the pom (the serde test dependencies). */
    static final DockerImageName IMAGE = DockerImageName.parse("apicurio/apicurio-registry:3.3.3");

    private GenericContainer<?> registry;

    @Override
    public Map<String, String> start() {
        registry = new GenericContainer<>(IMAGE)
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(2));
        registry.start();
        return Map.of("kafka-ui.registry.url", url(registry));
    }

    static String url(GenericContainer<?> c) {
        return "http://" + c.getHost() + ":" + c.getMappedPort(8080) + "/apis/registry/v3";
    }

    @Override
    public void stop() {
        if (registry != null) {
            registry.stop();
        }
    }
}
