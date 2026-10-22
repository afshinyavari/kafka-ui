package se.afshin.yavari.kafka.ui.produce;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import se.afshin.yavari.kafka.ui.config.IdLocation;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProperties;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistryException;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.registry.VersionSummary;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.serde.AvroCodec;
import se.afshin.yavari.kafka.ui.serde.AvroJsonException;
import se.afshin.yavari.kafka.ui.serde.WireFormat;

/** Writes one record to a topic; Kafka decides whether the caller may write. */
@ApplicationScoped
public class ProduceService {

    private static final long SEND_TIMEOUT_SECONDS = 30;
    private static final long CLOSE_TIMEOUT_SECONDS = 10;

    /** The number of partitions of an existing topic; fails with a user error when it does not exist. */
    @FunctionalInterface
    interface PartitionCount {
        int of(Credentials credentials, String topic);
    }

    private final Function<Credentials, Producer<byte[], byte[]>> producers;
    private final PartitionCount partitionCount;
    private final RegistryClient registry;
    private final KafkaUiConfig.Produce config;

    @Inject
    public ProduceService(KafkaClients clients, RegistryClient registry, KafkaUiConfig config) {
        this(clients::newProducer, (c, topic) -> partitionCount(clients, c, topic), registry, config.produce());
    }

    ProduceService(
            Function<Credentials, Producer<byte[], byte[]>> producers,
            PartitionCount partitionCount,
            RegistryClient registry,
            KafkaUiConfig.Produce config) {
        this.producers = producers;
        this.partitionCount = partitionCount;
        this.registry = registry;
        this.config = config;
    }

    /** Describes the topic (needs Describe, which any writer has) without the producer's auto-create metadata path. */
    private static int partitionCount(KafkaClients clients, Credentials credentials, String topic) {
        try {
            return clients.admin(credentials).describeTopics(List.of(topic)).allTopicNames()
                    .get(KafkaClientProperties.BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(topic).partitions().size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UserErrorException(KafkaErrors.translate(e, "topic " + topic));
        } catch (ExecutionException | java.util.concurrent.TimeoutException | RuntimeException e) {
            throw new UserErrorException(KafkaErrors.translate(e, "topic " + topic));
        }
    }

    /** Fails with "Topic X does not exist." (404) or an invalid-partition error (400) before anything is sent. */
    private void checkTarget(Credentials credentials, ProduceRequest req) {
        int partitions = partitionCount.of(credentials, req.topic());
        req.partition().filter(p -> p >= partitions).ifPresent(p -> {
            throw invalid("Partition " + p + " does not exist in topic " + req.topic() + " (it has " + partitions
                    + (partitions == 1 ? " partition" : " partitions") + ", numbered from 0).");
        });
    }

    public RecordMetadata produce(Credentials credentials, ProduceRequest req) {
        checkTarget(credentials, req);
        byte[] key = req.key() == null || req.key().isEmpty() ? null : req.key().getBytes(StandardCharsets.UTF_8);
        var headers = new RecordHeaders();
        for (var h : req.headers()) {
            if (h.key() != null && !h.key().isBlank()) {
                headers.add(h.key().trim(), (h.value() == null ? "" : h.value()).getBytes(StandardCharsets.UTF_8));
            }
        }
        byte[] value = req.mode() == ProduceMode.AVRO
                ? avroValue(credentials, req, headers)
                : (req.value() == null ? "" : req.value()).getBytes(StandardCharsets.UTF_8);

        Producer<byte[], byte[]> producer = null;
        try {
            producer = producers.apply(credentials);
            return producer
                    .send(new ProducerRecord<>(req.topic(), req.partition().orElse(null), null, key, value, headers))
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UserErrorException(KafkaErrors.translate(e, "topic " + req.topic()));
        } catch (ExecutionException | java.util.concurrent.TimeoutException | RuntimeException e) {
            if (e instanceof UserErrorException u) {
                throw u;
            }
            throw new UserErrorException(KafkaErrors.translate(e, "topic " + req.topic()));
        } finally {
            closeQuietly(producer);
        }
    }

    /** Bounded close: the default close() can block indefinitely when the broker is unreachable. */
    private static void closeQuietly(Producer<byte[], byte[]> producer) {
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS));
            } catch (RuntimeException e) {
                // the record outcome is already decided; a failing close must not mask it
            }
        }
    }

    private byte[] avroValue(Credentials credentials, ProduceRequest req, RecordHeaders headers) {
        SchemaVersionRef ref = req.valueSchema().orElseThrow(() -> invalid("Pick a schema version for the value."));
        try {
            RegistrySchema schema = registry.version(credentials, ref.groupId(), ref.artifactId(), ref.version());
            if (!"AVRO".equalsIgnoreCase(schema.type())) {
                throw invalid("Only Avro schemas can be used for producing.");
            }
            VersionSummary summary = registry.versions(credentials, ref.groupId(), ref.artifactId()).stream()
                    .filter(v -> v.version().equals(ref.version()))
                    .findFirst()
                    .orElseThrow(() -> new RegistryException(new Lookup.NotFound()));
            Schema avro = AvroCodec.parse(schema,
                    r -> registry.version(credentials, r.groupId(), r.artifactId(), r.version()).content());
            rejectFixed(avro, new java.util.HashSet<>());
            byte[] body = AvroCodec.fromJson(avro, req.value() == null ? "" : req.value());
            IdKind kind = config.idKind();
            long id = kind == IdKind.GLOBAL_ID ? summary.globalId() : summary.contentId();
            if (config.idLocation() == IdLocation.HEADERS) {
                WireFormat.writeHeader(headers, false, kind, id, config.keyHeaderPrefix(), config.valueHeaderPrefix());
                return body;
            }
            return WireFormat.frame(kind, id, body);
        } catch (AvroJsonException e) {
            String where = e.fieldPath() == null || e.fieldPath().isEmpty() ? "" : " (field: " + e.fieldPath() + ")";
            throw invalid("The value does not match the schema" + where + ": " + e.getMessage());
        } catch (RegistryException e) {
            throw new UserErrorException(registryError(e.failure()));
        }
    }

    /** The JSON-to-Avro converter cannot encode fixed fields. */
    private static void rejectFixed(Schema s, java.util.Set<Schema> seen) {
        if (!seen.add(s)) {
            return;
        }
        switch (s.getType()) {
            case FIXED -> throw new AvroJsonException(s.getFullName(), "fixed fields are not supported when producing");
            case RECORD -> s.getFields().forEach(f -> rejectFixed(f.schema(), seen));
            case ARRAY -> rejectFixed(s.getElementType(), seen);
            case MAP -> rejectFixed(s.getValueType(), seen);
            case UNION -> s.getTypes().forEach(t -> rejectFixed(t, seen));
            default -> { }
        }
    }

    private static UserErrorException invalid(String message) {
        return new UserErrorException(new UserError("Invalid message", message, 400));
    }

    private static UserError registryError(Lookup failure) {
        if (failure instanceof Lookup.NotFound) {
            return new UserError("Schema not found", "That schema version no longer exists in the registry.", 404);
        }
        if (failure instanceof Lookup.Denied) {
            return new UserError("Access denied", "You don't have permission to read that schema.", 403);
        }
        return new UserError("Schema registry unavailable", "The schema registry could not be reached.", 502);
    }
}
