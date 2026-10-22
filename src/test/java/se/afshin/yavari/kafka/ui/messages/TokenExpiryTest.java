package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

/** Streams end with {@code Expired} (and close their consumer) when the user's bearer token expires. */
@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class TokenExpiryTest {

    private static final Credentials MTLS = new Credentials.Mtls();

    @Inject KafkaClients clients;
    @Inject KafkaUiConfig config;

    private final MessageDecoder decoder = new MessageDecoder(
            (c, kind, id) -> {
                throw new IllegalStateException("no registry expected");
            },
            (c, r) -> {
                throw new IllegalStateException("no references expected");
            });

    private static Credentials.Bearer expiringInTwoSeconds() {
        return new Credentials.Bearer("t", "alice", Instant.now().plusSeconds(2));
    }

    private String topicWithRecords(String... values) throws Exception {
        String name = "expiry-" + UUID.randomUUID();
        clients.admin(MTLS).createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
        try (var producer = clients.newProducer(MTLS)) {
            for (String v : values) {
                producer.send(new ProducerRecord<>(name, 0, null, "k".getBytes(), v.getBytes())).get();
            }
        }
        return name;
    }

    /** Plaintext consumers (the bearer token is fake); {@code closed} counts down when closed. */
    private Function<Credentials, Consumer<byte[], byte[]>> consumers(CountDownLatch closed, boolean neverFinishes) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return c -> new KafkaConsumer<byte[], byte[]>(p) {
            @Override
            public long position(TopicPartition tp) {
                // a search whose range never counts as consumed keeps polling until the token expires
                return neverFinishes ? 0 : super.position(tp);
            }

            @Override
            public void close() {
                super.close();
                closed.countDown();
            }
        };
    }

    @Test
    void tailEndsWithExpiredWhenTokenExpires() throws Exception {
        String topic = topicWithRecords();
        CountDownLatch closed = new CountDownLatch(1);
        var service = new TailService(consumers(closed, false), decoder, Duration.ofMillis(100));
        var subscriber = service.tail(expiringInTwoSeconds(), topic, Set.of(), Optional.empty(), Optional.empty())
                .subscribe().withSubscriber(AssertSubscriber.<SearchEvent>create(Long.MAX_VALUE));
        subscriber.awaitItems(1, Duration.ofSeconds(30));
        Instant started = Instant.now();
        subscriber.awaitCompletion(Duration.ofSeconds(3));
        assertThat(Duration.between(started, Instant.now())).isLessThan(Duration.ofSeconds(3));
        assertThat(subscriber.getItems().getLast()).isEqualTo(new SearchEvent.Expired(0, 0, 0));
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void searchEndsWithExpiredAndCounts() throws Exception {
        String topic = topicWithRecords("a", "b", "c");
        CountDownLatch closed = new CountDownLatch(1);
        var service = new MessageSearchService(consumers(closed, true), decoder, config.search());
        Instant now = Instant.now();
        var request = new SearchRequest(topic, Set.of(), now.minus(Duration.ofHours(1)), now.plusSeconds(60),
                Optional.empty(), Optional.empty(), Optional.empty());
        var subscriber = service.search(expiringInTwoSeconds(), request)
                .subscribe().withSubscriber(AssertSubscriber.<SearchEvent>create(Long.MAX_VALUE));
        subscriber.awaitCompletion(Duration.ofSeconds(30));
        assertThat(subscriber.getItems().getLast()).isEqualTo(new SearchEvent.Expired(3, 3, 0));
        assertThat(subscriber.getItems()).noneMatch(e -> e instanceof SearchEvent.Done);
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
