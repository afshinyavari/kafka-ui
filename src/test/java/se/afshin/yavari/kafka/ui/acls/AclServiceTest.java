package se.afshin.yavari.kafka.ui.acls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

class AclServiceTest {

    private static AclBinding acl(String principal, ResourceType type, String name, AclOperation op) {
        return new AclBinding(new ResourcePattern(type, name, PatternType.LITERAL),
                new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW));
    }

    private static AclService serving(Object result) {
        return new AclService() {
            @Override
            @SuppressWarnings("unchecked")
            Collection<AclBinding> fetch(Credentials c) throws Exception {
                if (result instanceof Exception e) {
                    throw e;
                }
                return (Collection<AclBinding>) result;
            }
        };
    }

    private final List<AclBinding> sample = List.of(
            acl("User:alice", ResourceType.TOPIC, "orders", AclOperation.READ),
            acl("User:alice", ResourceType.GROUP, "g1", AclOperation.READ),
            acl("User:bob", ResourceType.TOPIC, "payments", AclOperation.WRITE));

    @Test void aclFilterByPrincipal() {
        var rows = serving(sample).list(new Credentials.Mtls(), new AclFilter("alice", null, null, null));
        assertThat(rows).extracting(AclRow::principal).containsOnly("User:alice").hasSize(2);
    }

    @Test void filtersByNameTypeAndOperation() {
        var svc = serving(sample);
        assertThat(svc.list(new Credentials.Mtls(), new AclFilter("", "PAY", "", ""))).hasSize(1);
        assertThat(svc.list(new Credentials.Mtls(), new AclFilter("", "", "GROUP", ""))).singleElement()
                .satisfies(r -> assertThat(r.resourceName()).isEqualTo("g1"));
        assertThat(svc.list(new Credentials.Mtls(), new AclFilter("", "", "", "WRITE"))).singleElement()
                .satisfies(r -> assertThat(r.principal()).isEqualTo("User:bob"));
        assertThat(svc.list(new Credentials.Mtls(), AclFilter.NONE)).hasSize(3);
    }

    @Test void securityDisabledIsFriendly() {
        assertThatThrownBy(() -> serving(new ExecutionException(new SecurityDisabledException("no")))
                .list(new Credentials.Mtls(), AclFilter.NONE))
                .isInstanceOf(UserErrorException.class)
                .hasMessage("ACLs are not enabled on this cluster.");
    }

    @Test void aclDeniedIsFriendly() {
        assertThatThrownBy(() -> serving(new ExecutionException(new ClusterAuthorizationException("no")))
                .list(new Credentials.Mtls(), AclFilter.NONE))
                .isInstanceOf(UserErrorException.class)
                .hasMessage("You don't have permission to access the cluster (ACLs).");
    }
}
