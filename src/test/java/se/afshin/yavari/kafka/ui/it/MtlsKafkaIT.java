package se.afshin.yavari.kafka.ui.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.acls.AclService;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;

/**
 * The app in mTLS mode, with its real client properties (no plaintext seam), against a broker that requires client
 * certificates and enforces ACLs.
 */
@QuarkusTest
@WithTestResource(SslKafkaResource.class)
class MtlsKafkaIT {

    private static final Credentials CREDS = new Credentials.Mtls();

    @Inject KafkaClients clients;

    @Test
    void mtlsModeTalksToSslKafka() throws Exception {
        clients.admin(CREDS).createTopics(List.of(new NewTopic("mtls-orders", 2, (short) 1))).all().get();

        given().get("/topics").then().statusCode(200)
                .body(containsString("/topics/mtls-orders"))
                .body(containsString("mTLS mode"));
    }

    @Test
    void aclsShownWithAuthorizer() throws Exception {
        AclBinding acl = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "acl-payments", PatternType.LITERAL),
                new AccessControlEntry("User:CN=alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
        clients.admin(CREDS).createAcls(List.of(acl)).all().get();

        given().get("/acls").then().statusCode(200)
                .body(not(containsString(AclService.DISABLED_MESSAGE)))
                .body(containsString("User:CN=alice"))
                .body(containsString("acl-payments"))
                .body(containsString("READ"));
    }
}
