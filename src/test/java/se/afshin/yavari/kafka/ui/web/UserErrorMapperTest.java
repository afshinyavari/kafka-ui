package se.afshin.yavari.kafka.ui.web;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MtlsProfile.class)
class UserErrorMapperTest {
    @Test void htmxRequestGetsFragmentNotFullPage() {
        given().header("HX-Request", "true").get("/test-errors/user").then().statusCode(403)
                .body(containsString("permission to access topic orders."))
                .body(not(containsString("<html")));
    }

    @Test void normalRequestGetsFullPage() {
        given().get("/test-errors/user").then().statusCode(403)
                .body(containsString("<html"))
                .body(containsString("href=\"/topics\""))
                .body(containsString("Access denied"));
    }

    @Test void unexpectedExceptionIsGenericAndLeaksNothing() {
        given().get("/test-errors/boom").then().statusCode(500)
                .body(containsString("Something went wrong"))
                .body(not(containsString("Exception")))
                .body(not(containsString("secret-detail")));
    }

    // Security tests: verify Quarkus' own specific mappers win over ExceptionMapper<Throwable> (no 500).
    @Test void unauthorizedIsHandledByQuarkusNot500() {
        given().redirects().follow(false).get("/test-errors/unauthorized").then().statusCode(401);
    }

    @Test void authenticationFailedIsHandledByQuarkusNot500() {
        given().redirects().follow(false).get("/test-errors/authfailed").then().statusCode(401);
    }

    @Test void forbiddenIsHandledByQuarkus403() {
        given().get("/test-errors/forbidden").then().statusCode(403);
    }

    @Test void unknownRouteIs404() {
        given().get("/test-errors/nope").then().statusCode(404);
    }
}
