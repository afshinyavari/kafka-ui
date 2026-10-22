package se.afshin.yavari.kafka.ui.acls;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class AclsResourceTest {

    @Test void aclsListed() {
        given().get("/acls").then().statusCode(200)
                .body(containsString("ACLs are not enabled on this cluster."))
                .body(containsString("<html"));
        given().header("HX-Request", "true").queryParam("principal", "x").get("/acls").then().statusCode(200)
                .body(containsString("ACLs are not enabled on this cluster."))
                .body(not(containsString("<html")));
    }

    @Test void aclsReadOnly() {
        given().get("/acls").then().statusCode(200)
                .body(not(containsString("Create ACL")))
                .body(not(containsString("Delete")));
    }
}
