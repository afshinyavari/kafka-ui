package se.afshin.yavari.kafka.ui.web;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OidcProfile.class)
class SessionResourceTest {

    @Test
    @TestSecurity(user = "alice")
    void pingReturns200ForLoggedInUser() {
        given().redirects().follow(false).get("/session/ping").then().statusCode(200);
    }

    @Test
    void pingRedirectsAnonymousUserToLogin() {
        given().redirects().follow(false).get("/session/ping").then().statusCode(302);
    }

    @Test
    @TestSecurity(user = "alice")
    void pingOmitsLifetimeWhenIdentityHasNoToken() {
        given().get("/session/ping").then().header("X-Token-Expires-In", org.hamcrest.Matchers.nullValue());
    }
}
