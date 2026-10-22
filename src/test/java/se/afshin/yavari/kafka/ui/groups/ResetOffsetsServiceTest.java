package se.afshin.yavari.kafka.ui.groups;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class ResetOffsetsServiceTest {

    private static final Credentials CREDS = new Credentials.Mtls();

    @Inject KafkaClients clients;
    @Inject ResetOffsetsService resets;
    @Inject GroupService groups;

    private ResetRequest request(String group, String topic, ResetTo to, Instant ts, Long offset) {
        return new ResetRequest(group, topic, Set.of(), to, Optional.ofNullable(ts), Optional.ofNullable(offset));
    }

    private long committed(String group, String topic) {
        return groups.describe(CREDS, group).offsets().stream()
                .filter(o -> o.topic().equals(topic)).findFirst().orElseThrow().committed();
    }

    private void prepare(String group, String topic, long commit) throws Exception {
        GroupFixtures.topicWith(clients, topic, 10);
        GroupFixtures.commit(group, topic, commit, false);
        GroupFixtures.awaitEmpty(clients, group);
    }

    @Test void resetToEarliest() throws Exception {
        prepare("rs-earliest-g", "rs-earliest", 6);
        var result = resets.reset(CREDS, request("rs-earliest-g", "rs-earliest", ResetTo.EARLIEST, null, null));
        assertThat(result).containsExactly(java.util.Map.entry(new TopicPartition("rs-earliest", 0), 0L));
        assertThat(committed("rs-earliest-g", "rs-earliest")).isZero();
    }

    @Test void resetToLatest() throws Exception {
        prepare("rs-latest-g", "rs-latest", 2);
        resets.reset(CREDS, request("rs-latest-g", "rs-latest", ResetTo.LATEST, null, null));
        assertThat(committed("rs-latest-g", "rs-latest")).isEqualTo(10);
    }

    @Test void resetToTimestamp() throws Exception {
        GroupFixtures.topicWith(clients, "rs-ts", 3);
        Thread.sleep(50);
        Instant middle = Instant.now();
        Thread.sleep(50);
        try (var producer = clients.newProducer(CREDS)) {
            for (int i = 0; i < 2; i++) {
                producer.send(new ProducerRecord<>("rs-ts", 0, null, "k".getBytes(), "late".getBytes())).get();
            }
        }
        GroupFixtures.commit("rs-ts-g", "rs-ts", 0, false);
        GroupFixtures.awaitEmpty(clients, "rs-ts-g");

        resets.reset(CREDS, request("rs-ts-g", "rs-ts", ResetTo.TIMESTAMP, middle, null));
        assertThat(committed("rs-ts-g", "rs-ts")).isEqualTo(3);

        // after the last record: no offset at/after the time, so the end offset
        resets.reset(CREDS, request("rs-ts-g", "rs-ts", ResetTo.TIMESTAMP, Instant.now().plusSeconds(3600), null));
        assertThat(committed("rs-ts-g", "rs-ts")).isEqualTo(5);
    }

    @Test void resetToOffsetIsClamped() throws Exception {
        prepare("rs-off-g", "rs-off", 1);
        resets.reset(CREDS, request("rs-off-g", "rs-off", ResetTo.OFFSET, null, 4L));
        assertThat(committed("rs-off-g", "rs-off")).isEqualTo(4);
        resets.reset(CREDS, request("rs-off-g", "rs-off", ResetTo.OFFSET, null, 999L));
        assertThat(committed("rs-off-g", "rs-off")).isEqualTo(10);
    }

    @Test void activeGroupIsRefused() throws Exception {
        GroupFixtures.topicWith(clients, "rs-active", 5);
        try (var member = GroupFixtures.commit("rs-active-g", "rs-active", 2, true)) {
            assertThatThrownBy(() -> resets.reset(CREDS,
                    request("rs-active-g", "rs-active", ResetTo.EARLIEST, null, null)))
                    .isInstanceOfSatisfying(UserErrorException.class, e -> {
                        assertThat(e.error().httpStatus()).isEqualTo(409);
                        assertThat(e.error().message())
                                .isEqualTo("Stop all consumers in group rs-active-g before resetting offsets.");
                    });
            assertThat(committed("rs-active-g", "rs-active")).isEqualTo(2);
        }
    }
}
