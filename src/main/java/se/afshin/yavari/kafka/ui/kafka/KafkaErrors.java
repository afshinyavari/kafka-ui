package se.afshin.yavari.kafka.ui.kafka;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/** Translates Kafka client failures into messages meant for end users. */
public final class KafkaErrors {
    private KafkaErrors() {
    }

    public static UserError translate(Throwable t, String resourceDescription) {
        var cause = unwrap(t);
        if (cause instanceof ClusterAuthorizationException) {
            return new UserError("Access denied", "You don't have permission to access the cluster (ACLs).", 403);
        }
        if (cause instanceof TopicAuthorizationException || cause instanceof GroupAuthorizationException) {
            return new UserError("Access denied",
                    "You don't have permission to access " + resourceDescription + ".", 403);
        }
        if (cause instanceof SaslAuthenticationException) {
            return new UserError("Session not accepted by Kafka",
                    "Kafka rejected your login token. Try reloading the page; if it persists, ask an administrator.",
                    401);
        }
        if (cause instanceof org.apache.kafka.common.errors.TimeoutException
                || cause instanceof java.util.concurrent.TimeoutException) {
            return new UserError("Kafka not responding", "Kafka did not respond in time.", 504);
        }
        if (cause instanceof UnknownTopicOrPartitionException) {
            return new UserError("Not found", capitalize(resourceDescription) + " does not exist.", 404);
        }
        return new UserError("Something went wrong",
                "An unexpected error occurred. Please try again; if it persists, ask an administrator.", 500);
    }

    private static Throwable unwrap(Throwable t) {
        var current = t;
        while ((current instanceof ExecutionException || current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String capitalize(String s) {
        return s == null || s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
