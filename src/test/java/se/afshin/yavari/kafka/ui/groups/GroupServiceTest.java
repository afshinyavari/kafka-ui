package se.afshin.yavari.kafka.ui.groups;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class GroupServiceTest {

    private static final Credentials CREDS = new Credentials.Mtls();

    @Inject KafkaClients clients;
    @Inject GroupService service;

    @Test void listShowsLag() throws Exception {
        GroupFixtures.topicWith(clients, "gs-lag", 10);
        GroupFixtures.commit("gs-lag-group", "gs-lag", 3, false);
        GroupFixtures.awaitEmpty(clients, "gs-lag-group");

        var rows = service.list(CREDS, "gs-lag-group");

        assertThat(rows).singleElement().satisfies(r -> {
            assertThat(r.groupId()).isEqualTo("gs-lag-group");
            assertThat(r.totalLag()).isEqualTo(7);
            assertThat(r.topics()).isEqualTo(1);
            assertThat(r.members()).isZero();
            assertThat(r.state()).isEqualTo("EMPTY");
        });
    }

    @Test void describeHasOffsetsMembersAndPermission() throws Exception {
        GroupFixtures.topicWith(clients, "gs-desc", 4);
        try (var member = GroupFixtures.commit("gs-desc-group", "gs-desc", 1, true)) {
            var d = service.describe(CREDS, "gs-desc-group");

            assertThat(d.state()).isEqualTo("STABLE");
            assertThat(d.members()).hasSize(1);
            assertThat(d.members().get(0).assignment()).isEqualTo("gs-desc-0");
            assertThat(d.offsets()).containsExactly(new OffsetRow("gs-desc", 0, 1, 4, 3));
            assertThat(d.canReset()).isTrue();
            assertThat(d.active()).isTrue();
        }
    }

    @Test void forTopicOnlyListsGroupsOnThatTopic() throws Exception {
        GroupFixtures.topicWith(clients, "gs-topic-a", 5);
        GroupFixtures.topicWith(clients, "gs-topic-b", 5);
        GroupFixtures.commit("gs-topic-group", "gs-topic-a", 2, false);

        assertThat(service.forTopic(CREDS, "gs-topic-a")).extracting(GroupRow::groupId).contains("gs-topic-group");
        assertThat(service.forTopic(CREDS, "gs-topic-b")).extracting(GroupRow::groupId)
                .doesNotContain("gs-topic-group");
    }
}
