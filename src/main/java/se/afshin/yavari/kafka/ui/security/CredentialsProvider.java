package se.afshin.yavari.kafka.ui.security;

import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.oidc.IdToken;
import io.quarkus.security.identity.SecurityIdentity;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Supplier;
import org.eclipse.microprofile.jwt.JsonWebToken;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;

/** Resolves the Kafka credentials of the current request. */
@RequestScoped
public class CredentialsProvider {

    private static final Duration OPAQUE_TOKEN_LIFETIME = Duration.ofMinutes(1);

    private final KafkaUiConfig config;
    private final SecurityIdentity identity;
    private final Supplier<String> verifiedSubject;

    @Inject @IdToken Instance<JsonWebToken> idToken;

    @Inject
    public CredentialsProvider(KafkaUiConfig config, SecurityIdentity identity) {
        this(config, identity, null);
    }

    /** @param verifiedSubject subject of the verified ID token, or null when there is none */
    CredentialsProvider(KafkaUiConfig config, SecurityIdentity identity, Supplier<String> verifiedSubject) {
        this.config = config;
        this.identity = identity;
        this.verifiedSubject = verifiedSubject != null ? verifiedSubject : this::idTokenSubject;
    }

    private String idTokenSubject() {
        return idToken.isResolvable() ? idToken.get().getSubject() : null;
    }

    public Credentials current() {
        if (config.mode() == Mode.MTLS) {
            return new Credentials.Mtls();
        }
        AccessTokenCredential access = identity.getCredential(AccessTokenCredential.class);
        if (access == null || access.getToken() == null) {
            throw new IllegalStateException("No OIDC access token available for the current user");
        }
        String subject = verifiedSubject.get();
        if (subject == null || subject.isBlank()) {
            throw new IllegalStateException("No verified OIDC subject available for the current user");
        }
        return new Credentials.Bearer(access.getToken(), subject, expiry(access.getToken(), Instant.now()));
    }

    /** Expiry from the (unverified) access token payload; only used for cache lifetime, never for identity. */
    static Instant expiry(String token, Instant now) {
        String[] parts = token.split("\\.");
        if (parts.length == 3) {
            try {
                JsonObject claims = new JsonObject(
                        new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
                Long exp = claims.getLong("exp");
                if (exp != null) {
                    return Instant.ofEpochSecond(exp);
                }
            } catch (RuntimeException e) {
                // opaque or malformed token: use the short fallback
            }
        }
        return now.plus(OPAQUE_TOKEN_LIFETIME);
    }
}
