package se.afshin.yavari.kafka.ui.messages;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.serde.WireFormat;

@QuarkusTest
@TestProfile(MessagesProfile.class)
@WithTestResource(TestCertsResource.class)
class MessagesResourceTest {

    private static final String TOPIC = "msg-detail";

    @Inject KafkaClients clients;

    static boolean created;

    private void ensureTopic() throws Exception {
        if (created) {
            return;
        }
        var creds = new Credentials.Mtls();
        try {
            clients.admin(creds).createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
        Schema schema = new Schema.Parser().parse(StubSchemaCache.ORDER);
        GenericRecord r = new GenericData.Record(schema);
        r.put("id", "o-42");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(r, enc);
        enc.flush();
        try (var producer = clients.newProducer(creds)) {
            // offset 0 of partition 0
            producer.send(new ProducerRecord<>(TOPIC, 0, "order-1".getBytes(),
                    WireFormat.frame(IdKind.CONTENT_ID, 7, out.toByteArray()))).get();
        }
        created = true;
    }

    @Test
    void tabDefaultsToLastHour() throws Exception {
        ensureTopic();
        String body = given().header("HX-Request", "true").get("/topics/" + TOPIC + "/messages").then()
                .statusCode(200)
                .body(not(containsString("<html")))
                .body(containsString("class=\"active\">Messages</a>"))
                .body(containsString("name=\"keyRegex\""))
                .body(containsString("name=\"valueRegex\""))
                .body(containsString("Seek by offset"))
                .body(containsString("Live"))
                .body(containsString("<option value=\"1\">1</option>"))
                .extract().asString();
        Matcher m = Pattern.compile("name=\"from\"[^>]*value=\"([^\"]+)\"").matcher(body);
        assertThat(m.find()).isTrue();
        Instant from = LocalDateTime.parse(m.group(1)).toInstant(ZoneOffset.UTC);
        assertThat(Duration.between(from, Instant.now().minus(Duration.ofHours(1))).abs())
                .isLessThan(Duration.ofMinutes(2));
    }

    @Test
    void fullPageTab() throws Exception {
        ensureTopic();
        given().get("/topics/" + TOPIC + "/messages").then().statusCode(200)
                .body(containsString("<html"))
                .body(containsString("name=\"valueRegex\""));
    }

    @Test
    void resultsFragmentConnectsToStream() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").queryParam("valueRegex", "o-4\\d")
                .get("/topics/" + TOPIC + "/messages/results").then().statusCode(200)
                .body(containsString("sse-connect=\"/topics/msg-detail/messages/search?"))
                .body(containsString("valueRegex=o-4%5Cd"))
                .body(containsString("sse-swap=\"match\""));
    }

    @Test
    void invalidRegexShowsInlineError() throws Exception {
        ensureTopic();
        given().header("HX-Request", "true").queryParam("valueRegex", "(unclosed")
                .get("/topics/" + TOPIC + "/messages/results").then().statusCode(200)
                .body(containsString("Invalid value regex"))
                .body(not(containsString("sse-connect")))
                .body(not(containsString("Exception")));
    }

    @Test
    void detailShowsExplanation() throws Exception {
        ensureTopic();
        given().get("/topics/" + TOPIC + "/messages/0/0").then().statusCode(200)
                .body(containsString("Avro – schema"))
                .body(containsString("from payload"))
                .body(containsString("o-42"))
                .body(containsString("order-1"))
                .body(containsString("language-json"));
    }

    @Test
    void decodeAsOverride() throws Exception {
        ensureTopic();
        given().queryParam("keyAs", "TEXT").queryParam("valueAs", "HEX")
                .get("/topics/" + TOPIC + "/messages/0/0").then().statusCode(200)
                .body(containsString("00000000  00 00 00 00 07"))
                .body(containsString("Shown as hex (chosen manually)"))
                .body(containsString("<option value=\"HEX\" selected>"));
    }

    @Test
    void missingMessageIsFriendlyNotFound() throws Exception {
        ensureTopic();
        given().get("/topics/" + TOPIC + "/messages/0/999").then().statusCode(404)
                .body(containsString("does not exist"))
                .body(not(containsString("Exception")));
    }

    @Test
    void doneEventShowsStopReasonAndCounts() throws Exception {
        ensureTopic();
        String body = given().accept("text/event-stream").queryParam("valueRegex", "o-4\\d")
                .get("/topics/" + TOPIC + "/messages/search").then().statusCode(200)
                .extract().asString();
        assertThat(body).contains("event:match").contains("/topics/msg-detail/messages/0/0")
                .contains("event:progress").contains("event:done")
                .contains("scanned").contains("end of range");
        // every data line of a multi-line payload is prefixed
        for (String line : body.split("\n")) {
            assertThat(line).matches("^(|event:.*|data:.*|id:.*|retry:.*|:.*)$");
        }
    }

    @Test
    void streamWithInvalidRegexEndsWithError() throws Exception {
        ensureTopic();
        String body = given().accept("text/event-stream").queryParam("keyRegex", "[")
                .get("/topics/" + TOPIC + "/messages/search").then().statusCode(200)
                .extract().asString();
        assertThat(body).contains("event:done").contains("Invalid key regex").doesNotContain("Exception");
    }
}
