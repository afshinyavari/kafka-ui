package se.afshin.yavari.kafka.ui.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.serde.Legacy8ByteIdHandler;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.config.IdLocation;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.produce.ProduceMode;
import se.afshin.yavari.kafka.ui.produce.ProduceRequest;
import se.afshin.yavari.kafka.ui.produce.ProduceService;
import se.afshin.yavari.kafka.ui.produce.SchemaVersionRef;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.serde.Decoded;
import se.afshin.yavari.kafka.ui.serde.Format;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;
import se.afshin.yavari.kafka.ui.serde.SchemaRef;

/**
 * Real Apicurio Registry 3 and its Kafka serdes: what they write is decoded automatically, what the UI produces is
 * read by Apicurio's deserializer, and the UI's produce defaults frame records exactly like the serde defaults.
 */
@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
@WithTestResource(ApicurioResource.class)
class ApicurioCompatibilityIT {

    private static final Credentials CREDS = new Credentials.Mtls();
    private static final ObjectMapper JSON = new ObjectMapper();

    static final String ORDER_SCHEMA = "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"it\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"},"
            + "{\"name\":\"note\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
    static final String ORDER_JSON = "{\"id\":\"o-1\",\"amount\":12.5,\"note\":\"rush\"}";
    static final String PERSON_SCHEMA = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\","
            + "\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}},"
            + "\"required\":[\"name\"]}";

    @ConfigProperty(name = "kafka-ui.registry.url") String registryUrl;

    @Inject MessageDecoder decoder;
    @Inject ProduceService produce;
    @Inject KafkaClients clients;
    @Inject KafkaUiConfig config;

    @Test
    void decodesAvroWrittenByApicurioSerdeDefaultConfig() throws Exception {
        Headers headers = new RecordHeaders();
        byte[] value = serialize(new AvroKafkaSerializer<>(), avroConfig(), "compat-default", headers, order());

        assertThat(apicurioHeaders(headers)).isEmpty();
        Decoded d = decoder.decode(CREDS, value, headers, false);

        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(JSON.readTree(d.text())).isEqualTo(JSON.readTree(ORDER_JSON));
        assertThat(d.ref()).get().extracting(SchemaRef::source, SchemaRef::kind)
                .containsExactly(SchemaRef.Source.PAYLOAD_4, IdKind.CONTENT_ID);
        assertThat(d.explanation()).contains("compat-default-value");
    }

    @Test
    void decodesAvroWrittenWithHeadersEnabled() throws Exception {
        Map<String, Object> cfg = avroConfig();
        cfg.put(KafkaSerdeConfig.ENABLE_HEADERS, true);
        Headers headers = new RecordHeaders();
        byte[] value = serialize(new AvroKafkaSerializer<>(), cfg, "compat-headers", headers, order());

        assertThat(apicurioHeaders(headers)).contains("apicurio.value.contentId");
        Decoded d = decoder.decode(CREDS, value, headers, false);

        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(JSON.readTree(d.text())).isEqualTo(JSON.readTree(ORDER_JSON));
        assertThat(d.ref()).get().extracting(SchemaRef::source, SchemaRef::kind)
                .containsExactly(SchemaRef.Source.HEADER, IdKind.CONTENT_ID);
    }

    @Test
    void decodesAvroWrittenWithGlobalIdHandler() throws Exception {
        Map<String, Object> cfg = avroConfig();
        cfg.put(SerdeConfig.ID_HANDLER, Legacy8ByteIdHandler.class.getName());
        cfg.put(SerdeConfig.USE_ID, "globalId");
        Headers headers = new RecordHeaders();
        byte[] value = serialize(new AvroKafkaSerializer<>(), cfg, "compat-global", headers, order());

        assertThat(apicurioHeaders(headers)).isEmpty();
        Decoded d = decoder.decode(CREDS, value, headers, false);

        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(JSON.readTree(d.text())).isEqualTo(JSON.readTree(ORDER_JSON));
        assertThat(d.ref()).get().extracting(SchemaRef::source, SchemaRef::kind)
                .containsExactly(SchemaRef.Source.PAYLOAD_8, IdKind.GLOBAL_ID);
    }

    @Test
    void decodesJsonSchemaWrittenByApicurioSerde() throws Exception {
        register("compat-json-value", "JSON", PERSON_SCHEMA);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerdeConfig.REGISTRY_URL, registryUrl);
        cfg.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);
        JsonNode person = JSON.readTree("{\"name\":\"Ada\",\"age\":36}");
        Headers headers = new RecordHeaders();
        byte[] value = serialize(new JsonSchemaKafkaSerializer<JsonNode>(), cfg, "compat-json", headers, person);

        Decoded d = decoder.decode(CREDS, value, headers, false);

        assertThat(d.format()).isEqualTo(Format.JSON_SCHEMA);
        assertThat(JSON.readTree(d.text())).isEqualTo(person);
        assertThat(d.explanation()).contains("compat-json-value");
    }

    @Test
    void apicurioDeserializerReadsOurAvroProduce() throws Exception {
        String topic = "compat-ui";
        register(topic + "-value", "AVRO", ORDER_SCHEMA);
        ConsumerRecord<byte[], byte[]> rec = produceAndRead(topic, new SchemaVersionRef("default", topic + "-value", "1"));

        try (var deserializer = new AvroKafkaDeserializer<GenericRecord>()) {
            deserializer.configure(Map.of(SerdeConfig.REGISTRY_URL, registryUrl), false);
            GenericRecord read = deserializer.deserialize(topic, rec.headers(), rec.value());
            assertThat(read).isEqualTo(order());
        }
    }

    @Test
    void produceDefaultsMatchApicurioSerdeDefaults() throws Exception {
        String topic = "compat-defaults";
        Headers serdeHeaders = new RecordHeaders();
        byte[] serdeValue = serialize(new AvroKafkaSerializer<>(), avroConfig(), topic, serdeHeaders, order());

        // the serde auto-registered <topic>-value v1; produce the same record from the UI against that version
        ConsumerRecord<byte[], byte[]> ours = produceAndRead(topic, new SchemaVersionRef("default", topic + "-value", "1"));

        assertThat(apicurioHeaders(ours.headers())).isEqualTo(apicurioHeaders(serdeHeaders)).isEmpty();
        assertThat(ours.value()).isEqualTo(serdeValue);
        assertThat(config.produce().idLocation()).isEqualTo(IdLocation.PAYLOAD);
        assertThat(config.produce().idKind()).isEqualTo(IdKind.CONTENT_ID);
    }

    // ---- helpers ----

    private Map<String, Object> avroConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerdeConfig.REGISTRY_URL, registryUrl);
        cfg.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, true);
        return cfg;
    }

    private static GenericRecord order() {
        GenericRecord r = new GenericData.Record(new Schema.Parser().parse(ORDER_SCHEMA));
        r.put("id", "o-1");
        r.put("amount", 12.5);
        r.put("note", "rush");
        return r;
    }

    private static <T> byte[] serialize(
            Serializer<T> serializer, Map<String, Object> cfg, String topic, Headers headers, T value) {
        try (serializer) {
            serializer.configure(cfg, false);
            return serializer.serialize(topic, headers, value);
        }
    }

    private static List<String> apicurioHeaders(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .map(Header::key)
                .filter(k -> k.startsWith("apicurio."))
                .toList();
    }

    private ConsumerRecord<byte[], byte[]> produceAndRead(String topic, SchemaVersionRef ref) throws Exception {
        clients.admin(CREDS).createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        produce.produce(CREDS, new ProduceRequest(
                topic, Optional.of(0), "k1", ORDER_JSON, List.of(), ProduceMode.AVRO, Optional.of(ref)));
        try (var consumer = clients.newConsumer(CREDS)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            for (int i = 0; i < 10; i++) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("no record on " + topic);
    }

    private void register(String artifactId, String type, String content) throws Exception {
        String body = JSON.writeValueAsString(Map.of(
                "artifactId", artifactId,
                "artifactType", type,
                "firstVersion", Map.of("content", Map.of("content", content, "contentType", "application/json"))));
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(registryUrl + "/groups/default/artifacts"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }
}
