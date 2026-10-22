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
    void layoutShowsBannerAndNavigation() {
        given().redirects().follow(false).get("/test-errors/user").then().statusCode(403)
                .body(containsString("mTLS mode – no login"))
                .body(containsString("href=\"/topics\""))
                .body(containsString("href=\"/consumers\""))
                .body(containsString("href=\"/schemas\""))
                .body(containsString("href=\"/acls\""));
    }

    @Test
    void dashboardIsReachableWithoutLoginAndFailsFastWhenKafkaIsDown() {
        // No broker listens in this profile: no login redirect, a friendly 504 page within the admin timeout.
        long start = System.nanoTime();
        given().redirects().follow(false).get("/").then().statusCode(504)
                .body(containsString("Kafka did not respond in time."))
                .body(containsString("mTLS mode – no login"));
        org.assertj.core.api.Assertions.assertThat(java.time.Duration.ofNanos(System.nanoTime() - start))
                .isLessThan(java.time.Duration.ofSeconds(25));
    }

    @Test
    void staticAssetsArePublic() {
        given().get("/vendor/htmx.min.js").then().statusCode(200);
        given().get("/css/app.css").then().statusCode(200);
    }
}
