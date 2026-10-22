package se.afshin.yavari.kafka.ui.produce;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistryException;
import se.afshin.yavari.kafka.ui.registry.SchemaCache;
import se.afshin.yavari.kafka.ui.registry.SchemaLookup;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.serde.Decoded;
import se.afshin.yavari.kafka.ui.serde.Format;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

/**
 * Guesses the value schema of a topic for the produce form: the schema of the newest message, else Apicurio's
 * default naming ({@code default / <topic>-value}). Every failure (no Read permission, empty topic, registry down)
 * just means no guess.
 */
@ApplicationScoped
public class SchemaDetector {

    private static final Logger LOG = Logger.getLogger(SchemaDetector.class);
    static final Duration READ_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration POLL = Duration.ofMillis(200);
    private static final String DEFAULT_GROUP = "default";

    /** The artifact to preselect and a sentence saying how it was found. */
    public record Detection(String groupId, String artifactId, String reason) {}

    private final Function<Credentials, Consumer<byte[], byte[]>> consumers;
    private final MessageDecoder decoder;
    private final SchemaLookup schemas;
    private final RegistryClient registry;

    @Inject
    public SchemaDetector(KafkaClients clients, MessageDecoder decoder, SchemaCache schemas, RegistryClient registry) {
        this(clients::newConsumer, decoder, schemas, registry);
    }

    SchemaDetector(Function<Credentials, Consumer<byte[], byte[]>> consumers, MessageDecoder decoder,
            SchemaLookup schemas, RegistryClient registry) {
        this.consumers = consumers;
        this.decoder = decoder;
        this.schemas = schemas;
        this.registry = registry;
    }

    public Optional<Detection> detect(Credentials credentials, String topic) {
        return fromLatestRecord(credentials, topic).or(() -> byNamingConvention(credentials, topic));
    }

    Optional<Detection> fromLatestRecord(Credentials credentials, String topic) {
        try {
            ConsumerRecord<byte[], byte[]> latest = latestRecord(credentials, topic);
            if (latest == null) {
                return Optional.empty();
            }
            Decoded value = decoder.decode(credentials, latest.value(), latest.headers(), false);
            if (value.format() != Format.AVRO || value.ref().isEmpty()) {
                return Optional.empty();
            }
            var ref = value.ref().get();
            if (schemas.get(credentials, ref.kind(), ref.id()) instanceof Lookup.Found f
                    && f.schema().artifactId() != null) {
                return Optional.of(new Detection(f.schema().groupId(), f.schema().artifactId(),
                        "Schema of the latest message (partition " + latest.partition() + ", offset "
                                + latest.offset() + ")."));
            }
        } catch (RuntimeException e) {
            LOG.debugf("No schema from the latest message of %s: %s", topic, e.toString());
        }
        return Optional.empty();
    }

    /** The newest record (by timestamp) among the last record of each partition; null when there is none. */
    private ConsumerRecord<byte[], byte[]> latestRecord(Credentials credentials, String topic) {
        try (Consumer<byte[], byte[]> consumer = consumers.apply(credentials)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, READ_TIMEOUT);
            if (infos == null || infos.isEmpty()) {
                return null;
            }
            List<TopicPartition> all = infos.stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            Map<TopicPartition, Long> begin = consumer.beginningOffsets(all, READ_TIMEOUT);
            Map<TopicPartition, Long> end = consumer.endOffsets(all, READ_TIMEOUT);
            Map<TopicPartition, Long> pending = new HashMap<>();
            for (TopicPartition tp : all) {
                if (end.getOrDefault(tp, 0L) > begin.getOrDefault(tp, 0L)) {
                    pending.put(tp, end.get(tp));
                }
            }
            if (pending.isEmpty()) {
                return null;
            }
            consumer.assign(new ArrayList<>(pending.keySet()));
            pending.forEach((tp, e) -> consumer.seek(tp, e - 1));
            ConsumerRecord<byte[], byte[]> latest = null;
            long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
            while (!pending.isEmpty() && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(POLL)) {
                    pending.remove(new TopicPartition(r.topic(), r.partition()));
                    if (latest == null || r.timestamp() > latest.timestamp()) {
                        latest = r;
                    }
                }
                // The last offset can be a transaction marker that yields no record; stop waiting once past it.
                pending.entrySet().removeIf(e -> consumer.position(e.getKey()) >= e.getValue());
            }
            return latest;
        }
    }

    Optional<Detection> byNamingConvention(Credentials credentials, String topic) {
        String artifact = topic + "-value";
        try {
            var versions = registry.versions(credentials, DEFAULT_GROUP, artifact);
            if (versions.isEmpty()) {
                return Optional.empty();
            }
            var latest = versions.stream().max(java.util.Comparator.comparingLong(v -> v.globalId())).get();
            if (!"AVRO".equalsIgnoreCase(registry.version(credentials, DEFAULT_GROUP, artifact, latest.version()).type())) {
                return Optional.empty();
            }
            return Optional.of(new Detection(DEFAULT_GROUP, artifact,
                    "Found by Apicurio's default naming (" + artifact + ")."));
        } catch (RegistryException e) {
            return Optional.empty();
        }
    }
}
