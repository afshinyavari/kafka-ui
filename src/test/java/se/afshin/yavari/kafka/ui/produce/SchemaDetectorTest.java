package se.afshin.yavari.kafka.ui.produce;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCerts;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;
import se.afshin.yavari.kafka.ui.serde.WireFormat;

class SchemaDetectorTest {

    private static final Credentials CREDS = new Credentials.Mtls();
    private static final String BASE = "/apis/registry/v3";
    private static final String TOPIC = "customers";
    private static final Schema SCHEMA = new Schema.Parser().parse(ProduceServiceTest.ORDER);

    @TempDir Path dir;

    private WireMockServer server;
    private TlsMaterialHolder holder;
    private RegistryClient registry;
    private MockConsumer<byte[], byte[]> consumer;
    private final Map<String, Lookup> lookups = new HashMap<>();

    @BeforeEach
    void setUp() {
        server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.start();
        holder = new TlsMaterialHolder(Optional.empty(), Optional.empty(), TestCerts.generate(dir).caFile());
        registry = new RegistryClient(URI.create("http://localhost:" + server.port() + BASE), holder);
        consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
        lookups.put("CONTENT_ID:7", new Lookup.Found(
                new RegistrySchema("AVRO", ProduceServiceTest.ORDER, "shop", "order-value", "3", List.of())));
    }

    @AfterEach
    void tearDown() {
        server.stop();
        holder.close();
    }

    private SchemaDetector detector() {
        var decoder = new MessageDecoder((c, kind, id) -> lookups.getOrDefault(kind + ":" + id, new Lookup.NotFound()),
                (c, r) -> {
                    throw new IllegalStateException("no references expected");
                });
        return new SchemaDetector(c -> consumer, decoder,
                (c, kind, id) -> lookups.getOrDefault(kind + ":" + id, new Lookup.NotFound()), registry);
    }

    private static byte[] avro() throws Exception {
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("id", "o-1");
        r.put("amount", 2.5);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(SCHEMA).write(r, enc);
        enc.flush();
        return WireFormat.frame(IdKind.CONTENT_ID, 7, out.toByteArray());
    }

    /** Two partitions; partition p ends at offset end[p], and its last record is {@code values[p]} at time ts[p]. */
    private void topic(long[] end, byte[][] values, long[] ts) {
        Node n = new Node(0, "localhost", 9092);
        consumer.updatePartitions(TOPIC, List.of(
                new PartitionInfo(TOPIC, 0, n, new Node[] {n}, new Node[] {n}),
                new PartitionInfo(TOPIC, 1, n, new Node[] {n}, new Node[] {n})));
        Map<TopicPartition, Long> begin = new HashMap<>();
        Map<TopicPartition, Long> ends = new HashMap<>();
        for (int p = 0; p < 2; p++) {
            begin.put(new TopicPartition(TOPIC, p), 0L);
            ends.put(new TopicPartition(TOPIC, p), end[p]);
        }
        consumer.updateBeginningOffsets(begin);
        consumer.updateEndOffsets(ends);
        consumer.schedulePollTask(() -> {
            for (int p = 0; p < 2; p++) {
                if (end[p] > 0) {
                    consumer.addRecord(new ConsumerRecord<>(TOPIC, p, end[p] - 1, ts[p],
                            org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, values[p].length, null,
                            values[p], new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty()));
                }
            }
        });
    }

    @Test
    void schemaOfTheNewestMessageAcrossPartitions() throws Exception {
        byte[] text = "{\"plain\":true}".getBytes(StandardCharsets.UTF_8);
        topic(new long[] {5, 9}, new byte[][] {text, avro()}, new long[] {1_000, 2_000});
        var d = detector().detect(CREDS, TOPIC);
        assertThat(d).isPresent();
        assertThat(d.get().groupId()).isEqualTo("shop");
        assertThat(d.get().artifactId()).isEqualTo("order-value");
        assertThat(d.get().reason()).isEqualTo("Schema of the latest message (partition 1, offset 8).");
    }

    @Test
    void newestMessageNotAvroFallsBackToNamingConvention() throws Exception {
        byte[] text = "{\"plain\":true}".getBytes(StandardCharsets.UTF_8);
        topic(new long[] {5, 9}, new byte[][] {text, avro()}, new long[] {3_000, 2_000});
        String art = BASE + "/groups/default/artifacts/" + TOPIC + "-value";
        server.stubFor(get(urlPathEqualTo(art + "/versions")).willReturn(okJson(
                "{\"versions\":[{\"version\":\"1\",\"globalId\":4,\"contentId\":2},"
                        + "{\"version\":\"2\",\"globalId\":5,\"contentId\":3}],\"count\":2}")));
        server.stubFor(get(urlEqualTo(art + "/versions/2/content")).willReturn(ok(ProduceServiceTest.ORDER)));
        server.stubFor(get(urlEqualTo(art + "/versions/2")).willReturn(okJson("{\"artifactType\":\"AVRO\"}")));
        server.stubFor(get(urlEqualTo(art + "/versions/2/references")).willReturn(okJson("[]")));
        var d = detector().detect(CREDS, TOPIC);
        assertThat(d).isPresent();
        assertThat(d.get().groupId()).isEqualTo("default");
        assertThat(d.get().artifactId()).isEqualTo("customers-value");
        assertThat(d.get().reason()).contains("default naming");
    }

    @Test
    void emptyTopicWithoutConventionArtifactHasNoGuess() {
        topic(new long[] {0, 0}, new byte[][] {null, null}, new long[] {0, 0});
        assertThat(detector().detect(CREDS, TOPIC)).isEmpty();
    }

    @Test
    void conventionArtifactThatIsNotAvroIsIgnored() {
        topic(new long[] {0, 0}, new byte[][] {null, null}, new long[] {0, 0});
        String art = BASE + "/groups/default/artifacts/" + TOPIC + "-value";
        server.stubFor(get(urlPathEqualTo(art + "/versions")).willReturn(okJson(
                "{\"versions\":[{\"version\":\"1\",\"globalId\":4,\"contentId\":2}],\"count\":1}")));
        server.stubFor(get(urlEqualTo(art + "/versions/1/content")).willReturn(ok("{}")));
        server.stubFor(get(urlEqualTo(art + "/versions/1")).willReturn(okJson("{\"artifactType\":\"JSON\"}")));
        server.stubFor(get(urlEqualTo(art + "/versions/1/references")).willReturn(okJson("[]")));
        assertThat(detector().detect(CREDS, TOPIC)).isEmpty();
    }
}
