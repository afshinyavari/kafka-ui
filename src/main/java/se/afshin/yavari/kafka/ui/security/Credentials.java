package se.afshin.yavari.kafka.ui.security;

import java.time.Instant;

/** Identity the UI uses towards Kafka: the shared client certificate, or the logged-in user's bearer token. */
public sealed interface Credentials permits Credentials.Mtls, Credentials.Bearer {

    /** Key that separates cached data between identities. */
    String cacheScope();

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

        /** Never print the token. */
        @Override
        public String toString() {
            return "Bearer[subject=" + subject + ", expiresAt=" + expiresAt + "]";
        }
    }
}
