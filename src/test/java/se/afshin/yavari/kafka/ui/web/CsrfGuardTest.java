package se.afshin.yavari.kafka.ui.web;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CsrfGuardTest {

    @Test void noOriginOrRefererPasses() {
        assertThat(CsrfGuard.sameOrigin("kafka-ui.example:8080", null, null)).isTrue();
    }

    @Test void matchingOriginPasses() {
        assertThat(CsrfGuard.sameOrigin("kafka-ui.example:8080", "http://kafka-ui.example:8080", null)).isTrue();
        assertThat(CsrfGuard.sameOrigin("Kafka-UI.example", "https://kafka-ui.example", null)).isTrue();
    }

    @Test void refererUsedWhenNoOrigin() {
        assertThat(CsrfGuard.sameOrigin("a:80", null, "http://a/topics/x")).isTrue();
        assertThat(CsrfGuard.sameOrigin("a:80", null, "http://b/topics/x")).isFalse();
    }

    @Test void foreignOriginFails() {
        assertThat(CsrfGuard.sameOrigin("a:8080", "http://a:9090", null)).isFalse();
        assertThat(CsrfGuard.sameOrigin("a", "http://evil.example", null)).isFalse();
        assertThat(CsrfGuard.sameOrigin("a", "null", null)).isFalse();
    }

    @Test void safeMethodsPassWithoutHeaders() {
        assertThat(CsrfGuard.decide("OPTIONS", null, "a", "http://evil.example", null)).isNull();
        assertThat(CsrfGuard.decide("HEAD", null, "a", null, null)).isNull();
        assertThat(CsrfGuard.decide("GET", null, "a", "http://evil.example", null)).isNull();
    }

    @Test void unsafeMethodsAreBlockedWithoutHxRequest() {
        for (String m : new String[] {"POST", "PUT", "DELETE", "PATCH"}) {
            var r = CsrfGuard.decide(m, null, "a", null, null);
            assertThat(r.getStatus()).isEqualTo(403);
            assertThat(r.getEntity().toString()).contains("Request blocked: cross-site request");
        }
    }

    @Test void matchingOriginWithoutHxRequestIsBlocked() {
        assertThat(CsrfGuard.decide("POST", null, "a:80", "http://a", null).getStatus()).isEqualTo(403);
        assertThat(CsrfGuard.decide("DELETE", "false", "a:80", "http://a", null).getStatus()).isEqualTo(403);
    }

    @Test void hxRequestWithSameOriginOrNoOriginPasses() {
        assertThat(CsrfGuard.decide("PUT", "true", "a:80", "http://a", null)).isNull();
        assertThat(CsrfGuard.decide("PUT", "true", "a:80", null, null)).isNull();
        assertThat(CsrfGuard.decide("PUT", "true", "a:80", "http://b", null).getStatus()).isEqualTo(403);
    }
}
