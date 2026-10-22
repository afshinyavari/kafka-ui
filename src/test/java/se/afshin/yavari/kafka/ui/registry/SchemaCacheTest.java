package se.afshin.yavari.kafka.ui.registry;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCerts;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;

class SchemaCacheTest {

    private static final String CONTENT = RegistryStubs.BASE + "/ids/contentIds/7";
    private static final Credentials ALICE = new Credentials.Bearer("a-tok", "alice", Instant.now().plusSeconds(60));
    private static final Credentials BOB = new Credentials.Bearer("b-tok", "bob", Instant.now().plusSeconds(60));

    @TempDir Path dir;

    private WireMockServer server;
    private TlsMaterialHolder holder;
    private SchemaCache cache;
    private Instant now = Instant.parse("2026-01-01T00:00:00Z");

    @BeforeEach
    void start() {
        server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.start();
        holder = new TlsMaterialHolder(Optional.empty(), Optional.empty(), TestCerts.generate(dir).caFile());
        var client = new RegistryClient(URI.create("http://localhost:" + server.port() + RegistryStubs.BASE), holder);
        cache = new SchemaCache(client, () -> now);
    }

    @AfterEach
    void stop() {
        server.stop();
        holder.close();
    }

    private int calls() {
        return server.findAll(getRequestedFor(urlEqualTo(CONTENT))).size();
    }

    @Test
    void foundIsCached() {
        RegistryStubs.schema(server);
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        assertThat(calls()).isEqualTo(1);
    }

    @Test
    void deniedIsRememberedOnlyBrieflyAndNeverAsNotFound() {
        server.stubFor(get(urlEqualTo(CONTENT)).willReturn(status(403)));
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isEqualTo(new Lookup.Denied(403));
        now = now.plus(Duration.ofSeconds(9));
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isEqualTo(new Lookup.Denied(403));
        assertThat(calls()).isEqualTo(1);

        server.resetAll();
        RegistryStubs.schema(server); // permission granted meanwhile
        now = now.plus(Duration.ofSeconds(2));
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        assertThat(calls()).isEqualTo(1);
    }

    @Test
    void unavailableIsRememberedOnlyBriefly() {
        server.stubFor(get(urlEqualTo(CONTENT)).willReturn(status(503)));
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Unavailable.class);
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(calls()).isEqualTo(1);
        now = now.plus(SchemaCache.FAILURE_TTL);
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(calls()).isEqualTo(2);
    }

    @Test
    void deniedMemoIsPerUser() {
        server.stubFor(get(urlEqualTo(CONTENT)).willReturn(status(403)));
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        server.resetAll();
        RegistryStubs.schema(server);
        assertThat(cache.get(BOB, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
    }

    @Test
    void sizeIsBoundedWithoutDroppingEverything() {
        RegistryStubs.schema(server);
        server.stubFor(get(urlMatching(RegistryStubs.BASE + "/ids/globalIds/.*")).willReturn(notFound()));
        var small = new SchemaCache(new RegistryClient(
                URI.create("http://localhost:" + server.port() + RegistryStubs.BASE), holder), () -> now, 5);
        for (int i = 1; i <= 20; i++) {
            small.get(ALICE, IdKind.GLOBAL_ID, i);
        }
        assertThat(small.size()).isLessThanOrEqualTo(5).isGreaterThan(0);
    }

    @Test
    void cacheIsPerUser() {
        RegistryStubs.schema(server);
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        server.stubFor(get(urlEqualTo(CONTENT))
                .withHeader("Authorization", equalTo("Bearer b-tok"))
                .willReturn(status(403)));
        assertThat(cache.get(BOB, IdKind.CONTENT_ID, 7)).isEqualTo(new Lookup.Denied(403));
        assertThat(calls()).isEqualTo(2);
    }

    @Test
    void idKindIsPartOfTheKey() {
        RegistryStubs.schema(server);
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        cache.get(ALICE, IdKind.GLOBAL_ID, 11);
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(server.findAll(getRequestedFor(urlEqualTo(RegistryStubs.BASE + "/ids/globalIds/11"))))
                .hasSize(1);
        assertThat(calls()).isEqualTo(1);
    }

    @Test
    void foundExpiresAfterTenMinutesAndNotFoundAfterOne() {
        server.stubFor(get(urlEqualTo(CONTENT)).willReturn(notFound()));
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        now = now.plus(Duration.ofSeconds(30));
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(calls()).isEqualTo(1);
        now = now.plus(Duration.ofSeconds(31));
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(calls()).isEqualTo(2);

        server.resetAll();
        RegistryStubs.schema(server);
        now = now.plus(Duration.ofMinutes(2));
        assertThat(cache.get(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        now = now.plus(Duration.ofMinutes(9));
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(calls()).isEqualTo(1);
        now = now.plus(Duration.ofMinutes(2));
        cache.get(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(calls()).isEqualTo(2);
    }
}
