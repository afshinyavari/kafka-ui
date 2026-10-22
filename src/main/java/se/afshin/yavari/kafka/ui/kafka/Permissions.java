package se.afshin.yavari.kafka.ui.kafka;

import java.util.Set;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;

/** Decides which actions to offer, based on the broker-reported authorized operations. */
public final class Permissions {
    private Permissions() {
    }

    public static boolean canProduce(TopicDescription topic) {
        return allows(topic.authorizedOperations(), AclOperation.WRITE);
    }

    public static boolean canResetOffsets(ConsumerGroupDescription group) {
        return allows(group.authorizedOperations(), AclOperation.READ);
    }

    /** A null set means the broker has no authorizer, so everything is allowed. */
    private static boolean allows(Set<AclOperation> ops, AclOperation needed) {
        return ops == null || ops.contains(needed) || ops.contains(AclOperation.ALL);
    }
}
