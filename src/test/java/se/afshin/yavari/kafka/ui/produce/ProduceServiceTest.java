package se.afshin.yavari.kafka.ui.produce;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.afshin.yavari.kafka.ui.config.IdLocation;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.messages.HeaderView;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCerts;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;
import se.afshin.yavari.kafka.ui.serde.Format;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

class ProduceServiceTest {

    private static final Credentials CREDS = new Credentials.Mtls();
    private static final String BASE = "/apis/registry/v3";
    static final String ORDER = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}";
    private static final String FIXED = "{\"type\":\"record\",\"name\":\"F\",\"fields\":["
            + "{\"name\":\"h\",\"type\":{\"type\":\"fixed\",\"name\":\"H\",\"size\":2}}]}";
    private static final SchemaVersionRef ORDER_REF = new SchemaVersionRef("default", "order-value", "3");

    @TempDir Path dir;

    private WireMockServer server;
    private TlsMaterialHolder holder;
    private RegistryClient registry;
    private MockProducer<byte[], byte[]> producer;

    @BeforeEach
    void setUp() {
        server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.start();
        stubArtifact("order-value", ORDER, 11, 7);
        stubArtifact("fixed-value", FIXED, 12, 8);
        holder = new TlsMaterialHolder(Optional.empty(), Optional.empty(), TestCerts.generate(dir).caFile());
        registry = new RegistryClient(URI.create("http://localhost:" + server.port() + BASE), holder);
        producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @AfterEach
    void tearDown() {
        server.stop();
        holder.close();
    }

    private void stubArtifact(String artifact, String content, long globalId, long contentId) {
        String art = BASE + "/groups/default/artifacts/" + artifact;
        server.stubFor(get(urlEqualTo(art + "/versions/3/content")).willReturn(ok(content)));
        server.stubFor(get(urlEqualTo(art + "/versions/3")).willReturn(okJson("{\"artifactType\":\"AVRO\"}")));
        server.stubFor(get(urlEqualTo(art + "/versions/3/references")).willReturn(okJson("[]")));
        server.stubFor(get(urlPathEqualTo(art + "/versions")).willReturn(okJson(
                "{\"versions\":[{\"version\":\"3\",\"globalId\":" + globalId + ",\"contentId\":" + contentId
                        + "}],\"count\":1}")));
    }

    private ProduceService service(IdLocation location, IdKind kind) {
        return new ProduceService(c -> producer, registry, new KafkaUiConfig.Produce() {
            @Override public IdLocation idLocation() { return location; }
            @Override public IdKind idKind() { return kind; }
            @Override public String valueHeaderPrefix() { return "apicurio.value."; }
            @Override public String keyHeaderPrefix() { return "apicurio.key."; }
        });
    }

    private static ProduceRequest raw(String value) {
        return new ProduceRequest("orders", Optional.of(0), "", value, List.of(), ProduceMode.RAW, Optional.empty());
    }

    private static ProduceRequest avro(String json, SchemaVersionRef ref) {
        return new ProduceRequest("orders", Optional.of(0), "k1", json, List.of(new HeaderView("trace", "abc")),
                ProduceMode.AVRO, Optional.of(ref));
    }

    @Test
    void rawXmlIsSentByteForByte() {
        service(IdLocation.PAYLOAD, IdKind.CONTENT_ID).produce(CREDS, raw("<a>1</a>"));
        ProducerRecord<byte[], byte[]> r = producer.history().get(0);
        assertThat(r.value()).isEqualTo("<a>1</a>".getBytes(StandardCharsets.UTF_8));
        assertThat(r.key()).isNull();
        assertThat(r.headers().toArray()).isEmpty();
    }

    @Test
    void avroWithPayloadFraming() {
        service(IdLocation.PAYLOAD, IdKind.CONTENT_ID).produce(CREDS, avro("{\"id\":\"o-1\",\"amount\":10.5}", ORDER_REF));
        ProducerRecord<byte[], byte[]> r = producer.history().get(0);
        assertThat(r.value()[0]).isZero();
        assertThat(ByteBuffer.wrap(r.value(), 1, 4).getInt()).isEqualTo(7);
        assertThat(r.key()).isEqualTo("k1".getBytes(StandardCharsets.UTF_8));
        assertThat(new String(r.headers().lastHeader("trace").value(), StandardCharsets.UTF_8)).isEqualTo("abc");
    }

    @Test
    void avroWithGlobalIdFraming() {
        service(IdLocation.PAYLOAD, IdKind.GLOBAL_ID).produce(CREDS, avro("{\"id\":\"o-1\",\"amount\":10.5}", ORDER_REF));
        byte[] v = producer.history().get(0).value();
        assertThat(v[0]).isZero();
        assertThat(ByteBuffer.wrap(v, 1, 8).getLong()).isEqualTo(11);
    }

    @Test
    void avroWithHeaders() {
        service(IdLocation.HEADERS, IdKind.CONTENT_ID).produce(CREDS, avro("{\"id\":\"o-1\",\"amount\":10.5}", ORDER_REF));
        ProducerRecord<byte[], byte[]> r = producer.history().get(0);
        Header h = r.headers().lastHeader("apicurio.value.contentId");
        assertThat(h).isNotNull();
        assertThat(ByteBuffer.wrap(h.value()).getLong()).isEqualTo(7);
        assertThat(r.value()).hasSize(12); // 1 length byte + "o-1" + 8-byte double, no id prefix
    }

    @Test
    void invalidJsonNamesTheField() {
        assertThatThrownBy(() -> service(IdLocation.PAYLOAD, IdKind.CONTENT_ID)
                .produce(CREDS, avro("{\"id\":\"o-1\"}", ORDER_REF)))
                .isInstanceOf(UserErrorException.class)
                .satisfies(e -> {
                    var err = ((UserErrorException) e).error();
                    assertThat(err.httpStatus()).isEqualTo(400);
                    assertThat(err.message()).contains("amount");
                });
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void fixedFieldsAreRejectedFriendly() {
        assertThatThrownBy(() -> service(IdLocation.PAYLOAD, IdKind.CONTENT_ID)
                .produce(CREDS, avro("{\"h\":\"ab\"}", new SchemaVersionRef("default", "fixed-value", "3"))))
                .isInstanceOf(UserErrorException.class)
                .hasMessageContaining("fixed fields are not supported when producing");
    }

    @Test
    void producedAvroDecodesBackToSameJson() {
        service(IdLocation.PAYLOAD, IdKind.CONTENT_ID).produce(CREDS, avro("{\"id\":\"o-1\",\"amount\":10.5}", ORDER_REF));
        ProducerRecord<byte[], byte[]> r = producer.history().get(0);
        var decoder = new MessageDecoder(
                (c, kind, id) -> new Lookup.Found(new RegistrySchema("AVRO", ORDER, "default", "order-value", "3", List.of())),
                (c, ref) -> {
                    throw new IllegalStateException();
                });
        var decoded = decoder.decode(CREDS, r.value(), r.headers(), false);
        assertThat(decoded.format()).isEqualTo(Format.AVRO);
        assertThat(decoded.text().replaceAll("\\s", "")).isEqualTo("{\"id\":\"o-1\",\"amount\":10.5}");
    }

    @Test
    void kafkaDenialIsFriendly() {
        var denying = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, org.apache.kafka.clients.producer.Callback cb) {
                return CompletableFuture.failedFuture(new TopicAuthorizationException(Set.of("orders")));
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
                return send(record, null);
            }
        };
        var svc = new ProduceService(c -> denying, registry, new KafkaUiConfig.Produce() {
            @Override public IdLocation idLocation() { return IdLocation.PAYLOAD; }
            @Override public IdKind idKind() { return IdKind.CONTENT_ID; }
            @Override public String valueHeaderPrefix() { return "apicurio.value."; }
            @Override public String keyHeaderPrefix() { return "apicurio.key."; }
        });
        assertThatThrownBy(() -> svc.produce(CREDS, raw("x")))
                .isInstanceOf(UserErrorException.class)
                .hasMessage("You don't have permission to access topic orders.");
    }
}
