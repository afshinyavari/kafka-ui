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
}
