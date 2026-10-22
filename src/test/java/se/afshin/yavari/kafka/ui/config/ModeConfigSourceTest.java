package se.afshin.yavari.kafka.ui.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ModeConfigSourceTest {
    @Test
    void mtlsDisablesOidcAndPermitsAll() {
        var p = ModeConfigSource.properties("mtls");
        assertEquals("false", p.get("quarkus.oidc.tenant-enabled"));
        assertEquals("permit", p.get("quarkus.http.auth.permission.app.policy"));
    }

    @Test
    void oidcEnablesOidcAndRequiresAuthentication() {
        var p = ModeConfigSource.properties("oidc");
        assertEquals("true", p.get("quarkus.oidc.tenant-enabled"));
        assertEquals("authenticated", p.get("quarkus.http.auth.permission.app.policy"));
    }

    @Test
    void unknownModeFailsClosed() {
        var p = ModeConfigSource.properties("bogus");
        assertEquals("true", p.get("quarkus.oidc.tenant-enabled"));
        assertEquals("authenticated", p.get("quarkus.http.auth.permission.app.policy"));
    }
}
