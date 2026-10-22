package se.afshin.yavari.kafka.ui.config;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.PropertiesConfigSource;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * Derives the OIDC tenant switch and the HTTP auth policy from {@code kafka-ui.mode}.
 * Registered as a {@link ConfigSourceFactory} so it can read the mode from all other sources.
 */
public class ModeConfigSource implements ConfigSourceFactory {
    static final int ORDINAL = 275;

    @Override
    public OptionalInt getPriority() {
        // sees every source with a lower ordinal than this; kept high so test/system overrides are visible
        return OptionalInt.of(1000);
    }

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        var value = context.getValue("kafka-ui.mode");
        String mode = value == null || value.getValue() == null ? "oidc" : value.getValue().trim().toLowerCase();
        return List.of(new PropertiesConfigSource(properties(mode), "kafka-ui-mode", ORDINAL));
    }

    static Map<String, String> properties(String mode) {
        boolean oidc = !mode.equals("mtls"); // fail closed: anything but mtls requires authentication
        return Map.of(
                "quarkus.oidc.tenant-enabled", String.valueOf(oidc),
                "quarkus.http.auth.permission.app.paths", "/*",
                "quarkus.http.auth.permission.app.policy", oidc ? "authenticated" : "permit");
    }
}
