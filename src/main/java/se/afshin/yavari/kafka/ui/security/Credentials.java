package se.afshin.yavari.kafka.ui.security;

import java.time.Instant;

/** Identity the UI uses towards Kafka: the shared client certificate, or the logged-in user's bearer token. */
public sealed interface Credentials permits Credentials.Mtls, Credentials.Bearer {

    /** Key that separates cached data between identities. */
    String cacheScope();

    /** True when these credentials can no longer be used at {@code now}; the shared client certificate never expires. */
    default boolean expiredAt(Instant now) {
        return false;
    }

    record Mtls() implements Credentials {
        @Override
        public String cacheScope() {
            return "mtls";
        }
    }

    record Bearer(String token, String subject, Instant expiresAt) implements Credentials {
        @Override
        public String cacheScope() {
            return "user:" + subject;
        }

        @Override
        public boolean expiredAt(Instant now) {
            return expiresAt != null && !now.isBefore(expiresAt);
        }

        /** Never print the token. */
        @Override
        public String toString() {
            return "Bearer[subject=" + subject + ", expiresAt=" + expiresAt + "]";
        }
    }
}
