package se.afshin.yavari.kafka.ui.web;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OidcProfile.class)
class LayoutOidcTest {
    @Test
    void anonymousUserIsRedirectedToKeycloak() {
        given().redirects().follow(false).get("/").then().statusCode(302)
                .header("Location", containsString("/protocol/openid-connect/auth"));
    }

    @Test
    @TestSecurity(user = "alice")
    void loggedInUserSeesNameAndNoBanner() {
        // "/" needs Kafka; an error page renders the same layout
        given().get("/test-errors/user").then().statusCode(403)
                .body(containsString("alice"))
                .body(not(containsString("mTLS mode")));
    }

    @Test
    void staticAssetsStayPublic() {
        given().redirects().follow(false).get("/css/app.css").then().statusCode(200);
    }
}
