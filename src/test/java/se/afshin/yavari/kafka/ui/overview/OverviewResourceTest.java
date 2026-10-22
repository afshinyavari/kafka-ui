package se.afshin.yavari.kafka.ui.overview;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

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
class OverviewResourceTest {

    @Inject KafkaClients clients;

    @Test void dashboardAndBrokers() throws Exception {
        var admin = clients.admin(new Credentials.Mtls());
        admin.createTopics(java.util.List.of(new org.apache.kafka.clients.admin.NewTopic("ov-topic", 3, (short) 1)))
                .all().get();
        String clusterId = admin.describeCluster().clusterId().get();
        int brokerId = admin.describeCluster().nodes().get().iterator().next().id();
        var all = admin.describeTopics(admin.listTopics(
                new org.apache.kafka.clients.admin.ListTopicsOptions().listInternal(true)).names().get())
                .allTopicNames().get().values();
        long topics = all.stream().filter(d -> !d.isInternal()).count();
        long partitions = all.stream().filter(d -> !d.isInternal()).mapToLong(d -> d.partitions().size()).sum();
        long leaders = all.stream().mapToLong(d -> d.partitions().size()).sum();

        given().get("/").then().statusCode(200)
                .body(containsString("<td id=\"cluster-id\">" + clusterId + "</td>"))
                .body(containsString("<td id=\"controller\">Broker " + brokerId + "</td>"))
                .body(containsString("<td id=\"broker-count\">1</td>"))
                .body(containsString("<td id=\"topic-count\">" + topics + "</td>"))
                .body(containsString("<td id=\"partition-count\">" + partitions + "</td>"))
                .body(containsString("<td id=\"urp-count\">0</td>"));
        String brokers = given().get("/brokers").then().statusCode(200).extract().asString();
        org.assertj.core.api.Assertions.assertThat(brokers).containsPattern(
                "<tr><td>" + brokerId + "</td><td>[^<]+:\\d+</td><td>-</td><td>" + leaders + "</td></tr>");
    }
}
