package se.afshin.yavari.kafka.ui.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;

class CredentialsProviderTest {

    private static String jwt(String payload) {
        var enc = Base64.getUrlEncoder().withoutPadding();
        return enc.encodeToString("{}".getBytes(StandardCharsets.UTF_8)) + "."
                + enc.encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".sig";
    }

    private static KafkaUiConfig cfg(Mode mode) {
        return new KafkaUiConfig() {
            public Mode mode() { return mode; }
            public String bootstrapServers() { return "b:1"; }
            public Tls tls() { throw new UnsupportedOperationException(); }
            public Registry registry() { throw new UnsupportedOperationException(); }
            public Search search() { throw new UnsupportedOperationException(); }
            public Tail tail() { throw new UnsupportedOperationException(); }
            public Produce produce() { throw new UnsupportedOperationException(); }
        };
    }

    private static SecurityIdentity identity(String accessToken) {
        var b = QuarkusSecurityIdentity.builder().setPrincipal(() -> "alice-name");
        if (accessToken != null) {
            b.addCredential(new AccessTokenCredential(accessToken));
        }
        return b.build();
    }

    @Test
    void mtlsModeReturnsMtls() {
        var p = new CredentialsProvider(cfg(Mode.MTLS), identity(null), (Supplier<String>) () -> null);
        assertThat(p.current()).isInstanceOf(Credentials.Mtls.class);
    }

    @Test
    void oidcSubjectComesFromVerifiedIdentityNotAccessTokenPayload() {
        String token = jwt("{\"sub\":\"forged\",\"exp\":2000000000}");
        var p = new CredentialsProvider(cfg(Mode.OIDC), identity(token), (Supplier<String>) () -> "u-1");
        var c = (Credentials.Bearer) p.current();
        assertThat(c.subject()).isEqualTo("u-1");
        assertThat(c.token()).isEqualTo(token);
        assertThat(c.expiresAt()).isEqualTo(Instant.ofEpochSecond(2000000000L));
    }

    @Test
    void oidcWithoutAccessTokenFails() {
        var p = new CredentialsProvider(cfg(Mode.OIDC), identity(null), (Supplier<String>) () -> "u-1");
        assertThatThrownBy(p::current).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void oidcWithoutVerifiedSubjectFails() {
        var p = new CredentialsProvider(cfg(Mode.OIDC), identity("opaque"), (Supplier<String>) () -> null);
        assertThatThrownBy(p::current).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void opaqueTokenGetsShortLifetime() {
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        assertThat(CredentialsProvider.expiry("opaque", now)).isEqualTo(now.plusSeconds(60));
    }
}
