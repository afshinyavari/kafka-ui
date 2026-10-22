package se.afshin.yavari.kafka.ui.web;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;

import io.quarkus.test.junit.QuarkusTest;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

@QuarkusTest
@io.quarkus.test.junit.TestProfile(MtlsProfile.class)
class AssetVersionTest {
    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void hashChangesWithContentAndIsStable() {
        String a = AssetVersion.hash(b("body{}"), b("x"));
        assertThat(a).hasSize(10).matches("[0-9a-f]{10}");
        assertThat(AssetVersion.hash(b("body{}"), b("x"))).isEqualTo(a);
        assertThat(AssetVersion.hash(b("body{color:red}"), b("x"))).isNotEqualTo(a);
        assertThat(AssetVersion.hash(b("body{}"), b("y"))).isNotEqualTo(a);
    }

    @Test
    void layoutLinksAssetsWithVersion() {
        String v = AssetVersion.current();
        assertThat(v).isNotBlank();
        given().redirects().follow(false).get("/test-errors/user").then()
                .body(containsString("/css/app.css?v=" + v))
                .body(containsString("/vendor/htmx.min.js?v=" + v))
                .body(containsString("/vendor/highlight.min.js?v=" + v))
                .body(containsString("/vendor/github.min.css?v=" + v));
    }
}
