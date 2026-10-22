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
        String clusterId = clients.admin(new Credentials.Mtls()).describeCluster().clusterId().get();
        given().get("/").then().statusCode(200)
                .body(containsString("<td id=\"cluster-id\">" + clusterId + "</td>"))
                .body(containsString("<td id=\"broker-count\">1</td>"))
                .body(containsString("id=\"controller\""))
                .body(containsString("id=\"topic-count\""))
                .body(containsString("id=\"partition-count\""))
                .body(containsString("<td id=\"urp-count\">0</td>"));
        given().get("/brokers").then().statusCode(200)
                .body(containsString("<th>Leader partitions</th>"))
                .body(containsString("<td>1</td>"));
    }
}
