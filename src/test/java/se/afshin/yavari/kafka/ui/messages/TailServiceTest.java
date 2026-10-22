package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class TailServiceTest {

    private static final Credentials CREDS = new Credentials.Mtls();

    @Inject KafkaClients clients;

    private final MessageDecoder decoder = new MessageDecoder(
            (c, kind, id) -> {
                throw new IllegalStateException("no registry expected");
            },
            (c, r) -> {
                throw new IllegalStateException("no references expected");
            });

    private TailService service() {
        return new TailService(clients::newConsumer, decoder, Duration.ofMillis(100));
    }

    private String topic() throws Exception {
        String name = "tail-" + UUID.randomUUID();
        clients.admin(CREDS).createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
        return name;
    }

    private void produce(String topic, String... values) throws Exception {
        try (var producer = clients.newProducer(CREDS)) {
            for (String v : values) {
                producer.send(new ProducerRecord<>(topic, 0, null, "k".getBytes(), v.getBytes())).get();
            }
        }
    }

    private AssertSubscriber<SearchEvent> start(TailService service, String topic, Optional<Pattern> value) {
        var subscriber = service.tail(CREDS, topic, Set.of(), Optional.empty(), value)
                .subscribe().withSubscriber(AssertSubscriber.<SearchEvent>create(Long.MAX_VALUE));
        // the first event reports that the consumer is positioned at the end of the topic
        subscriber.awaitItems(1, Duration.ofSeconds(30));
        assertThat(subscriber.getItems().getFirst()).isEqualTo(new SearchEvent.Progress(0, 0, 0));
        return subscriber;
    }

    private static List<String> values(AssertSubscriber<SearchEvent> s) {
        return s.getItems().stream().filter(e -> e instanceof SearchEvent.Match)
                .map(e -> ((SearchEvent.Match) e).message().value().text()).toList();
    }

    @Test
    void emitsOnlyRecordsProducedAfterStart() throws Exception {
        String topic = topic();
        produce(topic, "old1", "old2", "old3");
        var subscriber = start(service(), topic, Optional.empty());
        produce(topic, "new1", "new2");
        subscriber.awaitItems(3, Duration.ofSeconds(30));
        assertThat(values(subscriber)).containsExactly("new1", "new2");
        assertThat(subscriber.getItems()).noneMatch(e -> e instanceof SearchEvent.Done);
        subscriber.cancel();
    }

    @Test
    void appliesValueRegexAndReportsProgress() throws Exception {
        String topic = topic();
        var subscriber = start(service(), topic, Optional.of(Pattern.compile("^keep")));
        produce(topic, "drop-1", "keep-1", "drop-2");
        subscriber.awaitItems(3, Duration.ofSeconds(30)); // progress, match, progress (scanned 3)
        assertThat(values(subscriber)).containsExactly("keep-1");
        assertThat(subscriber.getItems()).contains(new SearchEvent.Progress(3, 1, 0));
        subscriber.cancel();
    }

    @Test
    void cancelClosesConsumer() throws Exception {
        String topic = topic();
        CountDownLatch closed = new CountDownLatch(1);
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        Function<Credentials, Consumer<byte[], byte[]>> watched = c -> new KafkaConsumer<byte[], byte[]>(p) {
            @Override
            public void close() {
                super.close();
                closed.countDown();
            }
        };
        var subscriber = start(new TailService(watched, decoder, Duration.ofMillis(100)), topic, Optional.empty());
        subscriber.cancel();
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
