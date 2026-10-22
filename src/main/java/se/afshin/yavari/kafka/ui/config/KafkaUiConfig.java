package se.afshin.yavari.kafka.ui.config;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.security.Mode;

@Unremovable
@ConfigMapping(prefix = "kafka-ui")
public interface KafkaUiConfig {

    Mode mode();

    String bootstrapServers();

    Tls tls();

    Registry registry();

    Search search();

    Tail tail();

    Produce produce();

    interface Tls {
        Optional<Path> certFile();

        Optional<Path> keyFile();

        Path caFile();
    }

    interface Registry {
        URI url();
    }

    interface Search {
        @WithDefault("100000")
        int maxScanned();

        @WithDefault("500")
        int maxMatches();

        @WithDefault("100ms")
        Duration regexTimeout();

        @WithDefault("1h")
        Duration defaultRange();
    }

    interface Tail {
        @WithDefault("1000")
        int maxRows();
    }

    /**
     * How produced Avro records carry their schema id. The defaults frame records exactly like Apicurio 3's serializer
     * defaults (magic byte + 4-byte contentId in the payload, no headers); pinned by ApicurioCompatibilityIT.
     */
    interface Produce {
        @WithDefault("PAYLOAD")
        IdLocation idLocation();

        @WithDefault("CONTENT_ID")
        IdKind idKind();

        @WithDefault("apicurio.value.")
        String valueHeaderPrefix();

        @WithDefault("apicurio.key.")
        String keyHeaderPrefix();
    }
}
