package se.afshin.yavari.kafka.ui.schemas;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;

@QuarkusTest
@TestProfile(SchemasProfile.class)
@WithTestResource(TestCertsResource.class)
class SchemasResourceTest {

    private static final String BASE = "/apis/registry/v3";
    private static WireMockServer registry;

    @BeforeAll
    static void start() {
        registry = new WireMockServer(WireMockConfiguration.options().port(SchemasProfile.REGISTRY_PORT));
        registry.start();
        String art = BASE + "/groups/default/artifacts/order-value";
        registry.stubFor(get(urlEqualTo(art + "/versions/3/content")).willReturn(ok(
                "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}")));
        registry.stubFor(get(urlEqualTo(art + "/versions/3")).willReturn(okJson("{\"artifactType\":\"AVRO\"}")));
        registry.stubFor(get(urlEqualTo(art + "/versions/3/references")).willReturn(okJson("[]")));
        registry.stubFor(get(urlPathEqualTo(art + "/versions")).willReturn(okJson(
                "{\"versions\":[{\"version\":\"2\",\"globalId\":10,\"contentId\":6},"
                        + "{\"version\":\"3\",\"globalId\":11,\"contentId\":7}],\"count\":2}")));
        registry.stubFor(get(urlPathEqualTo(BASE + "/search/artifacts")).willReturn(okJson(
                "{\"artifacts\":[{\"groupId\":\"default\",\"artifactId\":\"order-value\",\"name\":\"Order\","
                        + "\"artifactType\":\"AVRO\"},{\"groupId\":\"default\",\"artifactId\":\"j\","
                        + "\"name\":\"JsonThing\",\"artifactType\":\"JSON\"}],\"count\":2}")));
        String denied = BASE + "/groups/default/artifacts/secret";
        registry.stubFor(get(urlPathMatching(denied + "/.*")).willReturn(forbidden()));
        String down = BASE + "/groups/default/artifacts/broken";
        registry.stubFor(get(urlPathMatching(down + "/.*")).willReturn(serverError()));
    }

    @AfterAll
    static void stop() {
        registry.stop();
    }

    @Test void schemasSearchAndDetail() {
        given().get("/schemas").then().statusCode(200)
                .body(containsString("<a href=\"/schemas/default/order-value\">Order</a>"))
                .body(containsString("JsonThing"))
                .body(containsString("<span class=\"badge\">JSON</span>"))
                .body(containsString("<html"));
        given().header("HX-Request", "true").queryParam("q", "Order").get("/schemas").then().statusCode(200)
                .body(not(containsString("<html")));
        given().get("/schemas/default/order-value").then().statusCode(200)
                .body(containsString("AVRO"))
                .body(containsString("class=\"language-json\""))
                .body(containsString("&quot;name&quot;"))
                .body(containsString("/schemas/default/order-value?version=2"));
        given().queryParam("version", "3").get("/schemas/default/order-value").then().statusCode(200)
                .body(containsString("version 3"));
    }

    @Test void pickerStillAvroOnly() {
        given().get("/schemas/search").then().statusCode(200)
                .body(containsString("Order"))
                .body(not(containsString("JsonThing")));
    }

    @Test void schemaDeniedIsFriendly() {
        given().get("/schemas/default/secret").then().statusCode(403)
                .body(containsString("You don&#39;t have permission to read this schema."));
    }

    @Test void schemaUnavailableIsFriendly() {
        given().get("/schemas/default/broken").then().statusCode(502)
                .body(containsString("The schema registry could not be reached."));
    }

    @Test void schemasReadOnly() {
        given().get("/schemas/default/order-value").then().statusCode(200)
                .body(not(containsString("Create")))
                .body(not(containsString("Edit")))
                .body(not(containsString("Delete")));
        given().get("/schemas").then().body(not(containsString("Create"))).body(not(containsString("Delete")));
    }
}
