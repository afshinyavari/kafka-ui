package se.afshin.yavari.kafka.ui.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class CredentialsProviderTest {

    private static String jwt(String payload) {
        var enc = Base64.getUrlEncoder().withoutPadding();
        return enc.encodeToString("{}".getBytes(StandardCharsets.UTF_8)) + "."
                + enc.encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".sig";
    }

    @Test
    void readsSubjectAndExpiryFromAccessTokenClaims() {
        var b = CredentialsProvider.bearer(jwt("{\"sub\":\"u-1\",\"exp\":2000000000}"), "alice", Instant.now());
        assertThat(b.subject()).isEqualTo("u-1");
        assertThat(b.expiresAt()).isEqualTo(Instant.ofEpochSecond(2000000000L));
    }

    @Test
    void opaqueTokenFallsBackToPrincipalAndShortLifetime() {
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        var b = CredentialsProvider.bearer("opaque", "alice", now);
        assertThat(b.subject()).isEqualTo("alice");
        assertThat(b.expiresAt()).isEqualTo(now.plusSeconds(60));
    }
}
