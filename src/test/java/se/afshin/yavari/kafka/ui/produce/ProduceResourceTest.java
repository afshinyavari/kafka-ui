package se.afshin.yavari.kafka.ui.produce;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.topics.TopicInfo;

@QuarkusTest
@TestProfile(ProduceProfile.class)
@WithTestResource(TestCertsResource.class)
class ProduceResourceTest {

    private static final String TOPIC = "produce-ui";
    private static final String BASE = "/apis/registry/v3";
    private static WireMockServer registry;

    @Inject KafkaClients clients;
    @Inject @Location("TopicsResource/detail") Template detail;

    @BeforeAll
    static void startRegistry() {
        registry = new WireMockServer(WireMockConfiguration.options().port(ProduceProfile.REGISTRY_PORT));
        registry.start();
        String art = BASE + "/groups/default/artifacts/order-value";
        registry.stubFor(get(urlEqualTo(art + "/versions/3/content")).willReturn(ok(ProduceServiceTest.ORDER)));
        registry.stubFor(get(urlEqualTo(art + "/versions/3")).willReturn(okJson("{\"artifactType\":\"AVRO\"}")));
        registry.stubFor(get(urlEqualTo(art + "/versions/3/references")).willReturn(okJson("[]")));
        registry.stubFor(get(urlPathEqualTo(art + "/versions")).willReturn(okJson(
                "{\"versions\":[{\"version\":\"2\",\"globalId\":10,\"contentId\":6},"
                        + "{\"version\":\"3\",\"globalId\":11,\"contentId\":7}],\"count\":2}")));
        registry.stubFor(get(urlPathEqualTo(BASE + "/search/artifacts")).willReturn(okJson(
                "{\"artifacts\":[{\"groupId\":\"default\",\"artifactId\":\"order-value\",\"name\":\"Order\","
                        + "\"artifactType\":\"AVRO\"},{\"groupId\":\"default\",\"artifactId\":\"j\","
                        + "\"name\":\"JsonThing\",\"artifactType\":\"JSON\"}],\"count\":2}")));
    }

    @AfterAll
    static void stopRegistry() {
        registry.stop();
    }

    private void ensureTopic() throws Exception {
        try {
            clients.admin(new Credentials.Mtls()).createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    private byte[] lastValue() {
        try (var consumer = clients.newConsumer(new Credentials.Mtls())) {
            var tp = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            byte[] last = null;
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            while (consumer.position(tp) < end) {
                for (var r : consumer.poll(Duration.ofSeconds(2))) {
                    last = r.value();
                }
            }
            return last;
        }
    }

    @Test
    void rawProduceShowsPartitionOffsetAndLink() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").formParam("valueMode", "RAW").formParam("value", "<a>1</a>").formParam("key", "")
                .formParam("headerName", "trace").formParam("headerValue", "x")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(200)
                .body(containsString("partition 0"))
                .body(containsString("/topics/" + TOPIC + "/messages/0/"));
        assertThat(new String(lastValue())).isEqualTo("<a>1</a>");
    }

    @Test
    void avroProduceFramesWithContentId() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").formParam("valueMode", "AVRO").formParam("value", "{\"id\":\"o-1\",\"amount\":2.5}")
                .formParam("valueGroup", "default").formParam("valueArtifact", "order-value").formParam("valueVersion", "3")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(200);
        byte[] v = lastValue();
        assertThat(v[0]).isZero();
        assertThat(java.nio.ByteBuffer.wrap(v, 1, 4).getInt()).isEqualTo(7);
    }

    @Test
    void invalidJsonShowsFieldPath() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").formParam("valueMode", "AVRO").formParam("value", "{\"id\":\"o-1\"}")
                .formParam("valueGroup", "default").formParam("valueArtifact", "order-value").formParam("valueVersion", "3")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(400)
                .body(containsString("amount"))
                .body(not(containsString("<html")));
    }

    @Test
    void unknownTopicIsNotFoundAndNotCreated() throws Exception {
        given().header("HX-Request", "true").formParam("valueMode", "RAW").formParam("value", "x")
                .post("/topics/produce-ghost/produce").then().statusCode(404)
                .body(containsString("Topic produce-ghost does not exist."));
        assertThat(clients.admin(new Credentials.Mtls()).listTopics().names().get()).doesNotContain("produce-ghost");
    }

    @Test
    void partitionBeyondTheTopicIsAValidationError() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").formParam("valueMode", "RAW").formParam("value", "x")
                .formParam("partition", "5")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(400)
                .body(containsString("Partition 5 does not exist in topic " + TOPIC
                        + " (it has 1 partition, numbered from 0)."));
    }

    @Test
    void negativeOrNonNumericPartitionIsAValidationError() throws Exception {
        ensureTopic();
        for (String bad : new String[] {"-1", "abc"}) {
            given().header("HX-Request", "true").formParam("valueMode", "RAW").formParam("value", "x")
                    .formParam("partition", bad)
                    .post("/topics/" + TOPIC + "/produce").then().statusCode(400)
                    .body(containsString("Partition must be a non-negative number"));
        }
    }

    @Test
    void postWithoutHxRequestIsBlocked() throws Exception {
        ensureTopic();
        given().formParam("valueMode", "RAW").formParam("value", "x")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(403)
                .body(containsString("Request blocked: cross-site request"));
    }

    @Test
    void postWithForeignOriginIsBlocked() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").header("Origin", "https://evil.example")
                .formParam("valueMode", "RAW").formParam("value", "x")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(403)
                .body(containsString("Request blocked: cross-site request"));
        given().header("HX-Request", "true").header("Referer", "https://evil.example/page")
                .formParam("valueMode", "RAW").formParam("value", "x")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(403);
    }

    @Test
    void postWithMatchingOriginPasses() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").header("Origin", "http://localhost:" + io.restassured.RestAssured.port)
                .formParam("valueMode", "RAW").formParam("value", "x")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(200);
    }

    @Test
    void formFragmentLoadsTheSchemaPicker() {
        given().header("HX-Request", "true").get("/topics/" + TOPIC + "/produce").then().statusCode(200)
                // the form's hx-target is inherited; the loader must replace itself, not the result box
                .body(containsString("hx-get=\"/topics/" + TOPIC + "/produce/schema?role=key\" hx-trigger=\"load\" hx-target=\"this\""))
                .body(containsString("hx-get=\"/topics/" + TOPIC + "/produce/schema?role=value\" hx-trigger=\"load\" hx-target=\"this\""))
                .body(containsString("hx-post=\"/topics/" + TOPIC + "/produce\""))
                .body(containsString("name=\"keyMode\""))
                .body(containsString("name=\"keyTemplate\""))
                .body(containsString("name=\"valueTemplate\""))
                .body(containsString("<template id=\"produce-header-row\">"))
                .body(containsString("aria-label=\"Remove header\""));
    }

    @Test
    void schemaPickerPreselectsTheConventionArtifactAndAvro() throws Exception {
        try {
            clients.admin(new Credentials.Mtls()).createTopics(List.of(new NewTopic("order", 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
        String html = given().header("HX-Request", "true").get("/topics/order/produce/schema").then().statusCode(200)
                .body(containsString("default naming (order-value)"))
                .body(containsString("id=\"produce-value-mode\""))
                .body(containsString("hx-swap-oob=\"true\""))
                .body(containsString("<option value=\"AVRO\" selected>"))
                .body(containsString("name=\"valueArtifact\" value=\"order-value\""))
                .extract().asString();
        assertThat(html.indexOf("<option value=\"3\">")).isLessThan(html.indexOf("<option value=\"2\">"));
        // no order-key artifact: the key picker stays a plain search
        given().header("HX-Request", "true").get("/topics/order/produce/schema?role=key").then().statusCode(200)
                .body(containsString("hx-get=\"/schemas/search?role=key\""))
                .body(not(containsString("produce-key-mode")));
    }

    @Test
    void schemaPickerWithoutGuessIsThePlainSearch() {
        given().header("HX-Request", "true").get("/topics/" + TOPIC + "-nothing/produce/schema").then().statusCode(200)
                .body(containsString("hx-get=\"/schemas/search?role=value\""))
                .body(not(containsString("produce-value-mode")));
    }

    @Test
    void keyTemplateFillsTheKeyField() {
        given().header("HX-Request", "true").formParam("keyVersion", "3").formParam("key", "")
                .formParam("keyTemplate", "")
                .post("/schemas/default/order-value/template?role=key").then().statusCode(200)
                .body(containsString("<div id=\"produce-key\">"))
                .body(containsString("name=\"keyTemplate\""))
                .body(containsString("&quot;amount&quot; : 0.0"));
    }

    @Test
    void avroKeyIsFramed() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").formParam("keyMode", "AVRO").formParam("key", "{\"id\":\"k\",\"amount\":1}")
                .formParam("keyGroup", "default").formParam("keyArtifact", "order-value").formParam("keyVersion", "3")
                .formParam("valueMode", "RAW").formParam("value", "v")
                .post("/topics/" + TOPIC + "/produce").then().statusCode(200);
        try (var consumer = clients.newConsumer(new Credentials.Mtls())) {
            var tp = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(tp));
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            consumer.seek(tp, end - 1);
            var r = consumer.poll(Duration.ofSeconds(5)).iterator().next();
            assertThat(r.key()[0]).isZero();
            assertThat(java.nio.ByteBuffer.wrap(r.key(), 1, 4).getInt()).isEqualTo(7);
            assertThat(new String(r.value())).isEqualTo("v");
        }
    }

    @Test
    void templateFillsAnEmptyValueAndKeepsTypedText() {
        given().header("HX-Request", "true").formParam("valueVersion", "3").formParam("value", "")
                .formParam("valueTemplate", "")
                .post("/schemas/default/order-value/template").then().statusCode(200)
                .body(containsString("&quot;id&quot; : &quot;&quot;"))
                .body(containsString("&quot;amount&quot; : 0.0"));
        given().header("HX-Request", "true").formParam("valueVersion", "3").formParam("value", "{\"mine\":1}")
                .formParam("valueTemplate", "{\"old\":1}")
                .post("/schemas/default/order-value/template").then().statusCode(200)
                .body(containsString(">{&quot;mine&quot;:1}</textarea>"));
        given().header("HX-Request", "true").formParam("valueVersion", "3").formParam("value", "{\r\n\"old\":1}")
                .formParam("valueTemplate", "{\n\"old\":1}")
                .post("/schemas/default/order-value/template").then().statusCode(200)
                .body(containsString("&quot;amount&quot;"));
    }

    @Test
    void pickerListsOnlyAvroArtifactsAndVersionsNewestFirst() {
        given().get("/schemas/search?q=ord&role=key").then().statusCode(200)
                .body(containsString("/schemas/default/order-value/versions?role=key"))
                .body(containsString("hx-target=\"#key-version-picker\""))
                .body(not(containsString("JsonThing")));
        String html = given().get("/schemas/default/order-value/versions").then().statusCode(200)
                .body(containsString("name=\"valueArtifact\" value=\"order-value\""))
                .body(containsString("hx-target=\"#produce-value\""))
                .extract().asString();
        assertThat(html.indexOf("<option value=\"3\">")).isLessThan(html.indexOf("<option value=\"2\">"));
    }

    @Test
    void buttonDisabledWithoutWrite() {
        var info = new TopicInfo("orders", List.of(), Map.of(), false);
        String html = detail.data("name", "orders").data("tab", "overview").data("info", info).render();
        assertThat(html).containsPattern("<button[^>]*disabled[^>]*title=\"You don't have Write permission on this topic\"");
        var allowed = new TopicInfo("orders", List.of(), Map.of(), true);
        String ok = detail.data("name", "orders").data("tab", "overview").data("info", allowed).render();
        assertThat(ok).contains("hx-get=\"/topics/orders/produce\"").doesNotContain("Write permission");
    }
}
