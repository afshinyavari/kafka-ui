package se.afshin.yavari.kafka.ui.security;

import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.security.identity.SecurityIdentity;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;

/** Resolves the Kafka credentials of the current request. */
@RequestScoped
public class CredentialsProvider {

    private static final Duration OPAQUE_TOKEN_LIFETIME = Duration.ofMinutes(1);

    private final KafkaUiConfig config;
    private final SecurityIdentity identity;

    @Inject
    public CredentialsProvider(KafkaUiConfig config, SecurityIdentity identity) {
        this.config = config;
        this.identity = identity;
    }

    public Credentials current() {
        if (config.mode() == Mode.MTLS) {
            return new Credentials.Mtls();
        }
        AccessTokenCredential access = identity.getCredential(AccessTokenCredential.class);
        if (access == null || access.getToken() == null) {
            throw new IllegalStateException("No OIDC access token available for the current user");
        }
        return bearer(access.getToken(), identity.getPrincipal().getName(), Instant.now());
    }

    static Credentials.Bearer bearer(String token, String fallbackSubject, Instant now) {
        String subject = fallbackSubject;
        Instant expires = now.plus(OPAQUE_TOKEN_LIFETIME);
        String[] parts = token.split("\\.");
        if (parts.length == 3) {
            try {
                JsonObject claims = new JsonObject(
                        new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
                String sub = claims.getString("sub");
                if (sub != null && !sub.isBlank()) {
                    subject = sub;
                }
                Long exp = claims.getLong("exp");
                if (exp != null) {
                    expires = Instant.ofEpochSecond(exp);
                }
            } catch (RuntimeException e) {
                // opaque or malformed token: keep fallbacks
            }
        }
        return new Credentials.Bearer(token, subject, expires);
    }
}
