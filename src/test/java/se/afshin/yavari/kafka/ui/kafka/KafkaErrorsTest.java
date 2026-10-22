package se.afshin.yavari.kafka.ui.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

class KafkaErrorsTest {
    @Test void topicAuthorization() {
        var e = KafkaErrors.translate(new ExecutionException(new TopicAuthorizationException(Set.of("orders"))), "topic orders");
        assertThat(e.title()).isEqualTo("Access denied");
        assertThat(e.message()).isEqualTo("You don't have permission to access topic orders.");
        assertThat(e.httpStatus()).isEqualTo(403);
    }

    @Test void groupAuthorization() {
        var e = KafkaErrors.translate(new CompletionException(new GroupAuthorizationException("x")), "consumer group g1");
        assertThat(e.message()).isEqualTo("You don't have permission to access consumer group g1.");
        assertThat(e.httpStatus()).isEqualTo(403);
    }

    @Test void clusterAuthorization() {
        var e = KafkaErrors.translate(new ClusterAuthorizationException("x"), "the cluster (ACLs)");
        assertThat(e.message()).isEqualTo("You don't have permission to access the cluster (ACLs).");
        assertThat(e.httpStatus()).isEqualTo(403);
    }

    @Test void saslAuthentication() {
        var e = KafkaErrors.translate(new ExecutionException(new SaslAuthenticationException("x")), "topic orders");
        assertThat(e.title()).isEqualTo("Session not accepted by Kafka");
        assertThat(e.message()).isEqualTo("Kafka rejected your login token. Try reloading the page; if it persists, ask an administrator.");
        assertThat(e.httpStatus()).isEqualTo(401);
    }

    @Test void timeout() {
        var e = KafkaErrors.translate(new ExecutionException(new TimeoutException("x")), "topic orders");
        assertThat(e.message()).isEqualTo("Kafka did not respond in time.");
        assertThat(e.httpStatus()).isEqualTo(504);
    }

    @Test void unknownTopic() {
        var e = KafkaErrors.translate(new ExecutionException(new UnknownTopicOrPartitionException("x")), "topic orders");
        assertThat(e.message()).isEqualTo("Topic orders does not exist.");
        assertThat(e.httpStatus()).isEqualTo(404);
    }

    @Test void unknownExceptionHasNoStackTrace() {
        var e = KafkaErrors.translate(new IllegalStateException("boom at com.foo.Bar"), "topic orders");
        assertThat(e.httpStatus()).isEqualTo(500);
        assertThat(e.title() + e.message()).doesNotContain("Exception").doesNotContain("boom");
    }

    @Test void permissionsNullMeansAllowed() {
        assertThat(Permissions.canDescribeAcls(null)).isTrue();
        assertThat(Permissions.canDescribeAcls(Set.of(AclOperation.DESCRIBE))).isTrue();
        assertThat(Permissions.canDescribeAcls(Set.of(AclOperation.ALL))).isTrue();
        assertThat(Permissions.canDescribeAcls(Set.of(AclOperation.READ))).isFalse();
        assertThat(Permissions.canProduce(topic(null))).isTrue();
        assertThat(Permissions.canProduce(topic(Set.of(AclOperation.WRITE)))).isTrue();
        assertThat(Permissions.canProduce(topic(Set.of(AclOperation.READ)))).isFalse();
    }

    private static TopicDescription topic(Set<AclOperation> ops) {
        return new TopicDescription("t", false, java.util.List.of(), ops, Uuid.randomUuid());
    }
}
