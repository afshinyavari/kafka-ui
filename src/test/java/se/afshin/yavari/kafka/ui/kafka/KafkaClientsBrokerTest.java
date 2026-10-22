package se.afshin.yavari.kafka.ui.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;

/** Wiring check against the Dev Services broker through the PlaintextCustomizer seam. */
@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class KafkaClientsBrokerTest {

    @Inject KafkaClients clients;

    @Test
    void adminProducerAndConsumerReachTheBroker() throws Exception {
        var creds = new Credentials.Mtls();
        clients.admin(creds).createTopics(List.of(new NewTopic("t3-wiring", 1, (short) 1))).all().get();
        try (var producer = clients.producer(creds)) {
            producer.send(new ProducerRecord<>("t3-wiring", "k".getBytes(), "v".getBytes())).get();
        }
        try (var consumer = clients.newConsumer(creds)) {
            var tp = new org.apache.kafka.common.TopicPartition("t3-wiring", 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
        }
        assertThat(clients.admin(creds)).isSameAs(clients.admin(creds));
    }
}
