package se.afshin.yavari.kafka.ui.groups;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class GroupsResourceTest {

    @Inject KafkaClients clients;
    @Inject @Location("GroupsResource/detail") Template detail;

    private void group(String topic, String group, long commit) throws Exception {
        GroupFixtures.topicWith(clients, topic, 10);
        GroupFixtures.commit(group, topic, commit, false);
        GroupFixtures.awaitEmpty(clients, group);
    }

    @Test void listShowsGroupWithLag() throws Exception {
        group("gr-list", "gr-list-g", 3);
        given().get("/consumers").then().statusCode(200)
                .body(containsString("<a href=\"/consumers/gr-list-g\">gr-list-g</a>"))
                .body(containsString("<html"));
        given().header("HX-Request", "true").queryParam("q", "gr-list-g").get("/consumers").then().statusCode(200)
                .body(containsString("<td>7</td>"))
                .body(not(containsString("<html")));
    }

    @Test void detailShowsOffsetsAndEnabledResetButton() throws Exception {
        group("gr-detail", "gr-detail-g", 3);
        given().get("/consumers/gr-detail-g").then().statusCode(200)
                .body(containsString("/topics/gr-detail"))
                .body(containsString("hx-get=\"/consumers/gr-detail-g/reset\""));
    }

    @Test void unknownGroupIsNotFound() {
        given().get("/consumers/no-such-group").then().statusCode(404)
                .body(containsString("Consumer group no-such-group does not exist."));
    }

    @Test void resetFormAndResultFragment() throws Exception {
        group("gr-reset", "gr-reset-g", 3);
        given().header("HX-Request", "true").get("/consumers/gr-reset-g/reset").then().statusCode(200)
                .body(containsString("hx-confirm="))
                .body(containsString("<option value=\"gr-reset\">"))
                .body(containsString("type=\"datetime-local\""))
                .body(not(containsString("<html")));
        given().header("HX-Request", "true").formParam("topic", "gr-reset").formParam("to", "LATEST")
                .post("/consumers/gr-reset-g/reset").then().statusCode(200)
                .body(containsString("<td>gr-reset</td>"))
                .body(containsString("<td>10</td>"));
        given().get("/consumers/gr-reset-g").then().body(containsString("<td>10</td>"));
    }

    @Test void resetByTimestampAndOffsetFromForm() throws Exception {
        group("gr-form", "gr-form-g", 3);
        given().header("HX-Request", "true").formParam("topic", "gr-form").formParam("to", "OFFSET")
                .formParam("offset", "5").formParam("partitions", "0")
                .post("/consumers/gr-form-g/reset").then().statusCode(200).body(containsString("<td>5</td>"));
        given().header("HX-Request", "true").formParam("topic", "gr-form").formParam("to", "TIMESTAMP")
                .formParam("timestamp", "2020-01-01T00:00")
                .post("/consumers/gr-form-g/reset").then().statusCode(200).body(containsString("<td>0</td>"));
        given().header("HX-Request", "true").formParam("topic", "gr-form").formParam("to", "OFFSET")
                .formParam("offset", "x")
                .post("/consumers/gr-form-g/reset").then().statusCode(400);
    }

    @Test void activeGroupResetIsConflictWithMessage() throws Exception {
        GroupFixtures.topicWith(clients, "gr-active", 5);
        try (var member = GroupFixtures.commit("gr-active-g", "gr-active", 1, true)) {
            given().get("/consumers/gr-active-g").then().statusCode(200)
                    .body(containsString("Stop all consumers in group gr-active-g before resetting offsets."))
                    .body(not(containsString("hx-get=\"/consumers/gr-active-g/reset\"")));
            given().header("HX-Request", "true").formParam("topic", "gr-active").formParam("to", "EARLIEST")
                    .post("/consumers/gr-active-g/reset").then().statusCode(409)
                    .body(containsString("Stop all consumers in group gr-active-g before resetting offsets."));
        }
    }

    @Test void resetPostIsCsrfGuarded() throws Exception {
        group("gr-csrf", "gr-csrf-g", 3);
        given().formParam("topic", "gr-csrf").formParam("to", "LATEST")
                .post("/consumers/gr-csrf-g/reset").then().statusCode(403);
        given().header("HX-Request", "true").header("Origin", "https://evil.example")
                .formParam("topic", "gr-csrf").formParam("to", "LATEST")
                .post("/consumers/gr-csrf-g/reset").then().statusCode(403);
        given().header("HX-Request", "true").header("Origin", "http://localhost:" + io.restassured.RestAssured.port)
                .formParam("topic", "gr-csrf").formParam("to", "LATEST")
                .post("/consumers/gr-csrf-g/reset").then().statusCode(200);
    }

    @Test void resetButtonDisabledWithoutRead() {
        var denied = new GroupDetail("g1", "EMPTY", List.of(), List.of(), false);
        String html = detail.data("group", denied).render();
        assertThat(html).containsPattern(
                "<button[^>]*disabled[^>]*title=\"You don't have permission to reset offsets for this group\"");
        assertThat(html).doesNotContain("hx-get=\"/consumers/g1/reset\"");
        var allowed = new GroupDetail("g1", "EMPTY", List.of(), List.of(), true);
        assertThat(detail.data("group", allowed).render()).contains("hx-get=\"/consumers/g1/reset\"");
    }

    @Test void noDeleteGroupAction() throws Exception {
        group("gr-nodelete", "gr-nodelete-g", 3);
        given().get("/consumers").then().body(not(containsString("Delete")));
        given().get("/consumers/gr-nodelete-g").then().body(not(containsString("Delete")));
        given().header("HX-Request", "true").delete("/consumers/gr-nodelete-g").then().statusCode(405);
    }

    @Test void topicConsumersTab() throws Exception {
        group("gr-tab", "gr-tab-g", 3);
        given().header("HX-Request", "true").get("/topics/gr-tab/consumers").then().statusCode(200)
                .body(containsString("<a href=\"/consumers/gr-tab-g\">gr-tab-g</a>"))
                .body(containsString("class=\"active\">Consumers</a>"))
                .body(not(containsString("coming soon")));
    }

    @Test void groupIdsAreUrlEncodedInLinks() throws Exception {
        group("gr-enc", "gr enc&g", 3);
        given().get("/consumers").then().statusCode(200)
                .body(containsString("<a href=\"/consumers/gr%20enc%26g\">gr enc&amp;g</a>"));
        // the links are followed as written (already encoded)
        given().urlEncodingEnabled(false).get("/consumers/gr%20enc%26g").then().statusCode(200)
                .body(containsString("hx-get=\"/consumers/gr%20enc%26g/reset\""));
        given().urlEncodingEnabled(false).header("HX-Request", "true").get("/consumers/gr%20enc%26g/reset").then().statusCode(200)
                .body(containsString("hx-post=\"/consumers/gr%20enc%26g/reset\""));
        given().header("HX-Request", "true").get("/topics/gr-enc/consumers").then().statusCode(200)
                .body(containsString("<a href=\"/consumers/gr%20enc%26g\">"));
    }

    @Test void consumerPagesVaryOnHtmxHeaders() {
        given().get("/consumers").then().header("Vary", containsString("HX-Request"));
    }
}
