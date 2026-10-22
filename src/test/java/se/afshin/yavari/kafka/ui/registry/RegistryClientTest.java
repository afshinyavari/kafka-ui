package se.afshin.yavari.kafka.ui.registry;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCerts;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;

class RegistryClientTest {

    private static final Credentials ALICE = new Credentials.Bearer("tok", "alice", Instant.now().plusSeconds(60));

    @TempDir Path dir;

    private WireMockServer server;
    private TlsMaterialHolder holder;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
        if (holder != null) {
            holder.close();
        }
    }

    private RegistryClient plainClient() {
        server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.start();
        TestCerts.Generated g = TestCerts.generate(dir);
        holder = new TlsMaterialHolder(Optional.empty(), Optional.empty(), g.caFile());
        return new RegistryClient(URI.create("http://localhost:" + server.port() + RegistryStubs.BASE), holder);
    }

    /** HTTPS server requiring a client certificate signed by one of the generated CAs. */
    private RegistryClient mtlsClient(boolean presentClientCert) throws Exception {
        TestCerts.Generated g = TestCerts.generate(dir);
        char[] pw = "changeit".toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("server", g.serverKey().getPrivate(), pw, new Certificate[] {g.serverCert(), g.cas().get(1)});
        Path keystore = dir.resolve("server.p12");
        try (OutputStream out = Files.newOutputStream(keystore)) {
            ks.store(out, pw);
        }
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        for (int i = 0; i < g.cas().size(); i++) {
            ts.setCertificateEntry("ca" + i, g.cas().get(i));
        }
        Path truststore = dir.resolve("trust.p12");
        try (OutputStream out = Files.newOutputStream(truststore)) {
            ts.store(out, pw);
        }
        server = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .dynamicHttpsPort()
                .keystorePath(keystore.toString())
                .keystoreType("PKCS12")
                .keystorePassword("changeit")
                .keyManagerPassword("changeit")
                .needClientAuth(true)
                .trustStorePath(truststore.toString())
                .trustStoreType("PKCS12")
                .trustStorePassword("changeit"));
        server.start();
        RegistryStubs.schema(server);
        holder = presentClientCert
                ? new TlsMaterialHolder(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile())
                : new TlsMaterialHolder(Optional.empty(), Optional.empty(), g.caFile());
        return new RegistryClient(
                URI.create("https://localhost:" + server.httpsPort() + RegistryStubs.BASE), holder);
    }

    @Test
    void oidcSendsBearerToken() {
        RegistryClient client = plainClient();
        RegistryStubs.schema(server);
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        server.verify(getRequestedFor(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/7"))
                .withHeader("Authorization", equalTo("Bearer tok")));
        server.verify(getRequestedFor(urlEqualTo(RegistryStubs.BASE + "/search/versions?contentId=7"))
                .withHeader("Authorization", equalTo("Bearer tok")));
    }

    @Test
    void mtlsPresentsClientCertificate() throws Exception {
        RegistryClient client = mtlsClient(true);
        assertThat(client.byId(new Credentials.Mtls(), IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Found.class);
        server.verify(getRequestedFor(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/7"))
                .withoutHeader("Authorization"));
    }

    @Test
    void mtlsServerRejectsMissingClientCertificate() throws Exception {
        RegistryClient client = mtlsClient(false);
        assertThat(client.byId(new Credentials.Mtls(), IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Unavailable.class);
    }

    @Test
    void byContentIdReturnsSchemaWithMetadata() {
        RegistryClient client = plainClient();
        RegistryStubs.schema(server);
        Lookup l = client.byId(ALICE, IdKind.CONTENT_ID, 7);
        assertThat(l).isInstanceOfSatisfying(Lookup.Found.class, f -> {
            assertThat(f.schema().type()).isEqualTo("AVRO");
            assertThat(f.schema().artifactId()).isEqualTo("order-value");
            assertThat(f.schema().groupId()).isEqualTo("default");
            assertThat(f.schema().version()).isEqualTo("3");
            assertThat(f.schema().content()).isEqualTo(RegistryStubs.AVRO);
            assertThat(f.schema().references()).isEmpty();
        });
    }

    @Test
    void byGlobalIdUsesGlobalIdEndpoints() {
        RegistryClient client = plainClient();
        RegistryStubs.schema(server);
        assertThat(client.byId(ALICE, IdKind.GLOBAL_ID, 11)).isInstanceOf(Lookup.Found.class);
        server.verify(getRequestedFor(urlEqualTo(RegistryStubs.BASE + "/ids/globalIds/11")));
    }

    @Test
    void notFound404() {
        RegistryClient client = plainClient();
        server.stubFor(get(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/9")).willReturn(notFound()));
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 9)).isEqualTo(new Lookup.NotFound());
    }

    @Test
    void denied403And401() {
        RegistryClient client = plainClient();
        server.stubFor(get(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/9")).willReturn(status(403)));
        server.stubFor(get(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/10")).willReturn(status(401)));
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 9)).isEqualTo(new Lookup.Denied(403));
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 10)).isEqualTo(new Lookup.Denied(401));
    }

    @Test
    void serverErrorAndConnectionFailureAreUnavailable() {
        RegistryClient client = plainClient();
        server.stubFor(get(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/9")).willReturn(status(503)));
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 9)).isInstanceOf(Lookup.Unavailable.class);
        server.stop();
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOf(Lookup.Unavailable.class);
    }

    @Test
    void referencesAreFetchedForAvro() {
        RegistryClient client = plainClient();
        RegistryStubs.schema(server);
        server.stubFor(get(urlEqualTo(RegistryStubs.BASE + "/ids/contentIds/7/references"))
                .willReturn(okJson("[{\"groupId\":null,\"artifactId\":\"address\",\"version\":\"1\","
                        + "\"name\":\"com.acme.Address\"}]")));
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOfSatisfying(Lookup.Found.class, f -> {
            assertThat(f.schema().references()).hasSize(1);
            var ref = f.schema().references().get(0);
            assertThat(ref.name()).isEqualTo("com.acme.Address");
            assertThat(ref.groupId()).isEqualTo("default");
            assertThat(ref.artifactId()).isEqualTo("address");
            assertThat(ref.version()).isEqualTo("1");
        });
    }

    @Test
    void searchArtifactsParsesPage() {
        RegistryClient client = plainClient();
        server.stubFor(get(urlPathEqualTo(RegistryStubs.BASE + "/search/artifacts"))
                .withQueryParam("name", equalTo("order"))
                .withQueryParam("offset", equalTo("5"))
                .withQueryParam("limit", equalTo("10"))
                .willReturn(okJson("{\"artifacts\":[{\"groupId\":null,\"artifactId\":\"order-value\","
                        + "\"name\":\"Order\",\"artifactType\":\"AVRO\",\"modifiedOn\":\"2026-01-02T03:04:05Z\"}],"
                        + "\"count\":42}")));
        Page<ArtifactSummary> page = client.searchArtifacts(ALICE, "order", 5, 10);
        assertThat(page.total()).isEqualTo(42);
        assertThat(page.offset()).isEqualTo(5);
        assertThat(page.limit()).isEqualTo(10);
        assertThat(page.items()).containsExactly(new ArtifactSummary(
                "default", "order-value", "Order", "AVRO", Instant.parse("2026-01-02T03:04:05Z")));
    }

    @Test
    void versionsAndVersionContent() {
        RegistryClient client = plainClient();
        String a = RegistryStubs.BASE + "/groups/default/artifacts/order-value/versions";
        server.stubFor(get(urlPathEqualTo(a)).willReturn(okJson("{\"versions\":[{\"version\":\"3\",\"globalId\":11,"
                + "\"contentId\":7,\"createdOn\":\"2026-01-02T03:04:05Z\",\"state\":\"ENABLED\"}],\"count\":1}")));
        server.stubFor(get(urlEqualTo(a + "/3/content")).willReturn(ok(RegistryStubs.AVRO)));
        server.stubFor(get(urlEqualTo(a + "/3")).willReturn(okJson("{\"groupId\":null,\"artifactId\":\"order-value\","
                + "\"version\":\"3\",\"artifactType\":\"AVRO\",\"globalId\":11,\"contentId\":7}")));
        server.stubFor(get(urlEqualTo(a + "/3/references")).willReturn(okJson("[]")));
        assertThat(client.versions(ALICE, null, "order-value"))
                .containsExactly(new VersionSummary("3", 11, 7, Instant.parse("2026-01-02T03:04:05Z"), "ENABLED"));
        RegistrySchema s = client.version(ALICE, null, "order-value", "3");
        assertThat(s.type()).isEqualTo("AVRO");
        assertThat(s.content()).isEqualTo(RegistryStubs.AVRO);
        assertThat(s.version()).isEqualTo("3");
    }

    @Test
    void versionsPagesThroughAllResults() {
        RegistryClient client = plainClient();
        String a = RegistryStubs.BASE + "/groups/default/artifacts/big/versions";
        server.stubFor(get(urlPathEqualTo(a)).withQueryParam("offset", equalTo("0")).willReturn(okJson(versionPage(1, 100, 130))));
        server.stubFor(get(urlPathEqualTo(a)).withQueryParam("offset", equalTo("100")).willReturn(okJson(versionPage(101, 130, 130))));
        var all = client.versions(ALICE, null, "big");
        assertThat(all).hasSize(130);
        assertThat(all.get(129).version()).isEqualTo("130");
    }

    private static String versionPage(int from, int to, int total) {
        StringBuilder sb = new StringBuilder("{\"versions\":[");
        for (int i = from; i <= to; i++) {
            sb.append(i > from ? "," : "").append("{\"version\":\"").append(i)
                    .append("\",\"globalId\":").append(i).append(",\"contentId\":").append(i)
                    .append(",\"state\":\"ENABLED\"}");
        }
        return sb.append("],\"count\":").append(total).append("}").toString();
    }

    @Test
    void artifactIdIsPathEncoded() {
        RegistryClient client = plainClient();
        server.stubFor(get(urlPathEqualTo(RegistryStubs.BASE + "/groups/default/artifacts/a%2Fb%3Fc/versions"))
                .willReturn(okJson("{\"versions\":[],\"count\":0}")));
        assertThat(client.versions(ALICE, null, "a/b?c")).isEmpty();
    }

    @Test
    void sharedContentIdPicksHighestGlobalId() {
        RegistryClient client = plainClient();
        RegistryStubs.schema(server);
        server.stubFor(get(urlEqualTo(RegistryStubs.BASE + "/search/versions?contentId=7")).willReturn(okJson(
                "{\"versions\":[{\"groupId\":\"g\",\"artifactId\":\"new\",\"version\":\"2\",\"artifactType\":\"AVRO\",\"globalId\":20},"
                + "{\"groupId\":\"g\",\"artifactId\":\"old\",\"version\":\"1\",\"artifactType\":\"AVRO\",\"globalId\":5}],\"count\":2}")));
        assertThat(client.byId(ALICE, IdKind.CONTENT_ID, 7)).isInstanceOfSatisfying(Lookup.Found.class,
                f -> assertThat(f.schema().artifactId()).isEqualTo("new"));
    }

    @Test
    void failuresOutsideLookupThrowRegistryException() {
        RegistryClient client = plainClient();
        server.stubFor(get(urlPathEqualTo(RegistryStubs.BASE + "/search/artifacts")).willReturn(status(403)));
        org.junit.jupiter.api.Assertions.assertThrows(
                RegistryException.class, () -> client.searchArtifacts(ALICE, "", 0, 10));
    }
}
