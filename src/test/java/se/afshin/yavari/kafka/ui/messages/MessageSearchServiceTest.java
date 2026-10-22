package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;
import se.afshin.yavari.kafka.ui.serde.WireFormat;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class MessageSearchServiceTest {

    private static final Credentials CREDS = new Credentials.Mtls();
    private static final String ORDER = """
            {"type":"record","name":"Order","namespace":"shop","fields":[{"name":"id","type":"string"}]}""";
    private static final Schema SCHEMA = new Schema.Parser().parse(ORDER);

    @Inject KafkaClients clients;

    private final MessageDecoder decoder = new MessageDecoder(
            (c, kind, id) -> kind == IdKind.CONTENT_ID && id == 42
                    ? new Lookup.Found(new RegistrySchema("AVRO", ORDER, "shop", "orders-value", "1", List.of()))
                    : new Lookup.NotFound(),
            (c, r) -> {
                throw new IllegalStateException("no references expected");
            });

    private static KafkaUiConfig.Search limits(int maxScanned, int maxMatches) {
        return new KafkaUiConfig.Search() {
            @Override
            public int maxScanned() {
                return maxScanned;
            }

            @Override
            public int maxMatches() {
                return maxMatches;
            }

            @Override
            public Duration regexTimeout() {
                return Duration.ofMillis(100);
            }

            @Override
            public Duration defaultRange() {
                return Duration.ofHours(1);
            }
        };
    }

    private MessageSearchService service(int maxScanned, int maxMatches) {
        return new MessageSearchService(clients::newConsumer, decoder, limits(maxScanned, maxMatches));
    }

    private MessageSearchService service() {
        return service(100_000, 500);
    }

    private String topic(int partitions) throws Exception {
        String name = "search-" + UUID.randomUUID();
        clients.admin(CREDS).createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        return name;
    }

    private void produce(String topic, Long timestamp, String key, byte[] value) throws Exception {
        try (var producer = clients.newProducer(CREDS)) {
            producer.send(new ProducerRecord<>(topic, 0, timestamp,
                    key == null ? null : key.getBytes(StandardCharsets.UTF_8), value)).get();
        }
    }

    private void produce(String topic, String key, String value) throws Exception {
        produce(topic, null, key, value.getBytes(StandardCharsets.UTF_8));
    }

    private void produceMany(String topic, int n) throws Exception {
        try (var producer = clients.newProducer(CREDS)) {
            for (int i = 0; i < n; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null, ("k" + i).getBytes(), ("v" + i).getBytes()));
            }
            producer.flush();
        }
    }

    private static SearchRequest lastHour(String topic, String keyRegex, String valueRegex) {
        Instant now = Instant.now();
        return new SearchRequest(topic, Set.of(), now.minus(Duration.ofHours(1)), now.plusSeconds(60), Optional.empty(),
                Optional.ofNullable(keyRegex).map(Pattern::compile), Optional.ofNullable(valueRegex).map(Pattern::compile));
    }

    private static List<SearchEvent> run(MessageSearchService service, SearchRequest request) {
        return service.search(CREDS, request).collect().asList().await().atMost(Duration.ofSeconds(60));
    }

    private static List<MessageView> matches(List<SearchEvent> events) {
        return events.stream().filter(e -> e instanceof SearchEvent.Match).map(e -> ((SearchEvent.Match) e).message())
                .toList();
    }

    private static SearchEvent.Done done(List<SearchEvent> events) {
        assertThat(events).isNotEmpty();
        assertThat(events.getLast()).isInstanceOf(SearchEvent.Done.class);
        return (SearchEvent.Done) events.getLast();
    }

    private static byte[] avroOrder(String id) throws Exception {
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("id", id);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(SCHEMA).write(r, enc);
        enc.flush();
        return WireFormat.frame(IdKind.CONTENT_ID, 42, out.toByteArray());
    }

    @Test
    void onlyMessagesInsideTimeRange() throws Exception {
        String topic = topic(1);
        Instant t = Instant.now();
        produce(topic, t.minus(Duration.ofHours(2)).toEpochMilli(), "k", "old".getBytes());
        produce(topic, t.minus(Duration.ofMinutes(30)).toEpochMilli(), "k", "mid".getBytes());
        produce(topic, t.minus(Duration.ofMinutes(10)).toEpochMilli(), "k", "new".getBytes());
        var events = run(service(), new SearchRequest(topic, Set.of(), t.minus(Duration.ofHours(1)), t,
                Optional.empty(), Optional.empty(), Optional.empty()));
        assertThat(matches(events)).extracting(m -> m.value().text()).containsExactly("mid", "new");
        var done = done(events);
        assertThat(done.matched()).isEqualTo(2);
        assertThat(done.stopReason()).isEqualTo("end of range");
    }

    @Test
    void valueRegexMatchesDecodedAvroJson() throws Exception {
        String topic = topic(1);
        produce(topic, null, "a", avroOrder("o-42"));
        produce(topic, null, "b", avroOrder("o-13"));
        var events = run(service(), lastHour(topic, null, "o-4\\d"));
        assertThat(matches(events)).hasSize(1);
        assertThat(matches(events).getFirst().value().explanation()).startsWith("Avro – schema");
        assertThat(done(events).scanned()).isEqualTo(2);
    }

    @Test
    void keyAndValueRegexBothMustMatch() throws Exception {
        String topic = topic(1);
        produce(topic, "order-1", "{\"status\":\"paid\"}");
        produce(topic, "order-2", "{\"status\":\"open\"}");
        produce(topic, "refund-1", "{\"status\":\"paid\"}");
        var events = run(service(), lastHour(topic, "^order", "paid"));
        assertThat(matches(events)).extracting(m -> m.key().text()).containsExactly("order-1");
    }

    @Test
    void scanLimitStopsWithReason() throws Exception {
        String topic = topic(1);
        produceMany(topic, 50);
        var done = done(run(service(10, 500), lastHour(topic, null, null)));
        assertThat(done.scanned()).isEqualTo(10);
        assertThat(done.stopReason()).isEqualTo("scan limit reached (10 messages)");
    }

    @Test
    void matchLimitStopsWithReason() throws Exception {
        String topic = topic(1);
        produceMany(topic, 50);
        var events = run(service(100_000, 5), lastHour(topic, null, null));
        assertThat(matches(events)).hasSize(5);
        assertThat(done(events).stopReason()).isEqualTo("match limit reached (5)");
    }

    @Test
    void regexTimeoutsAreCountedNotFatal() throws Exception {
        String topic = topic(1);
        for (int i = 0; i < 4; i++) {
            produce(topic, "k", "aaa");
        }
        produce(topic, "k", "a".repeat(5_000) + "!");
        var events = run(service(), lastHour(topic, null, "(a+)+$"));
        var done = done(events);
        assertThat(done.regexTimeouts()).isEqualTo(1);
        assertThat(done.matched()).isEqualTo(4);
        assertThat(matches(events)).hasSize(4);
        assertThat(done.stopReason()).isEqualTo("end of range");
    }

    @Test
    void cancellationClosesConsumer() throws Exception {
        String topic = topic(1);
        produceMany(topic, 200);
        CountDownLatch closed = new CountDownLatch(1);
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        Function<Credentials, Consumer<byte[], byte[]>> slow = c -> new KafkaConsumer<byte[], byte[]>(p) {
            @Override
            public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.poll(timeout);
            }

            @Override
            public void close() {
                super.close();
                closed.countDown();
            }
        };
        var service = new MessageSearchService(slow, decoder, limits(100_000, 500));
        var subscriber = service.search(CREDS, lastHour(topic, null, null))
                .subscribe().withSubscriber(AssertSubscriber.<SearchEvent>create(Long.MAX_VALUE));
        subscriber.awaitItems(1, Duration.ofSeconds(30));
        subscriber.cancel();
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(subscriber.getItems()).noneMatch(e -> e instanceof SearchEvent.Done);
    }

    @Test
    void stopsWhenKafkaMakesNoProgress() throws Exception {
        String topic = topic(1);
        produceMany(topic, 5);
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        Function<Credentials, Consumer<byte[], byte[]>> silent = c -> new KafkaConsumer<byte[], byte[]>(p) {
            @Override
            public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ConsumerRecords.empty();
            }
        };
        var service = new MessageSearchService(silent, decoder, limits(100_000, 500), Duration.ofSeconds(1));
        var done = done(run(service, lastHour(topic, null, null)));
        assertThat(done.stopReason()).isEqualTo("no progress from Kafka for 1 s");
        assertThat(done.scanned()).isZero();
        assertThat(MessageSearchService.IDLE_TIMEOUT).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void emptyRangeEndsWithZero() throws Exception {
        String topic = topic(2);
        var events = run(service(), lastHour(topic, null, null));
        assertThat(events).containsExactly(new SearchEvent.Progress(0, 0, 0), new SearchEvent.Done(0, 0, 0, "end of range"));
    }

    @Test
    void onlySelectedPartitionsAndOffsetSeek() throws Exception {
        String topic = topic(1);
        produceMany(topic, 10);
        Instant now = Instant.now();
        var events = run(service(), new SearchRequest(topic, Set.of(0), now.minus(Duration.ofDays(1)), now.plusSeconds(60),
                Optional.of(7L), Optional.empty(), Optional.empty()));
        assertThat(matches(events)).extracting(MessageView::offset).containsExactly(7L, 8L, 9L);
    }

    @Test
    void fetchOneFindsExactRecord() throws Exception {
        String topic = topic(1);
        produceMany(topic, 5);
        var m = service().fetch(CREDS, topic, 0, 3, null, null);
        assertThat(m).hasValueSatisfying(v -> assertThat(v.value().text()).isEqualTo("v3"));
        assertThat(service().fetch(CREDS, topic, 0, 99, null, null)).isEmpty();
    }
}
