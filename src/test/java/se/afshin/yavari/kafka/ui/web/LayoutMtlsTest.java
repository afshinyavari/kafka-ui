package se.afshin.yavari.kafka.ui.web;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MtlsProfile.class)
class LayoutMtlsTest {
    @Test
    void dashboardIsReachableWithoutLoginAndShowsBanner() {
        given().redirects().follow(false).get("/").then().statusCode(200)
                .body(containsString("mTLS mode – no login"))
                .body(containsString("href=\"/topics\""))
                .body(containsString("href=\"/consumers\""))
                .body(containsString("href=\"/schemas\""))
                .body(containsString("href=\"/acls\""));
    }

    @Test
    void staticAssetsArePublic() {
        given().get("/vendor/htmx.min.js").then().statusCode(200);
        given().get("/css/app.css").then().statusCode(200);
    }
}
