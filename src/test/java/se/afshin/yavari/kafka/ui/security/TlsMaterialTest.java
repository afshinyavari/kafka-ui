package se.afshin.yavari.kafka.ui.security;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsMaterialTest {

    @TempDir Path dir;

    @Test
    void loadsPkcs8RsaKey() throws Exception {
        var g = TestCerts.generate(dir);
        var m = TlsMaterial.load(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile());
        assertTrue(m.clientKeyPem().contains("BEGIN PRIVATE KEY"));
        assertTrue(m.clientCertChainPem().contains("BEGIN CERTIFICATE"));
        assertNotNull(m.sslContext());
    }

    @Test
    void loadsPkcs8EcKey() throws Exception {
        var g = TestCerts.generate(dir);
        KeyPair ec = TestCerts.ec();
        X509Certificate cert = TestCerts.sign("CN=ec-client", ec, g.cas().get(0).getSubjectX500Principal().getName(),
                g.caKeys().get(0), false, false);
        Files.writeString(dir.resolve("ec.crt"), TestCerts.pem(cert));
        Files.writeString(dir.resolve("ec.key"), TestCerts.pkcs8Pem(ec.getPrivate()));
        var m = TlsMaterial.load(Optional.of(dir.resolve("ec.crt")), Optional.of(dir.resolve("ec.key")), g.caFile());
        assertNotNull(m.sslContext());
        assertTrue(m.clientKeyPem().contains("BEGIN PRIVATE KEY"));
    }

    @Test
    void nonPkcs8KeyFailsWithPathInMessage() throws Exception {
        var g = TestCerts.generate(dir);
        Path bad = dir.resolve("pkcs1.key");
        Files.writeString(bad, TestCerts.pkcs1Pem(g.clientKey().getPrivate()));
        assertTrue(Files.readString(bad).contains("BEGIN RSA PRIVATE KEY"));
        var e = assertThrows(IllegalStateException.class,
                () -> TlsMaterial.load(Optional.of(g.certFile()), Optional.of(bad), g.caFile()));
        assertTrue(e.getMessage().contains(bad.toString()), e.getMessage());
        assertTrue(e.getMessage().contains("PKCS#8"), e.getMessage());
    }

    @Test
    void caBundleWithThreeCertsLoadsAllThree() {
        var g = TestCerts.generate(dir, 3, 1);
        var m = TlsMaterial.load(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile());
        assertEquals(3, m.caCertificates().size());
        assertTrue(m.caBundlePem().contains("BEGIN CERTIFICATE"));
    }

    @Test
    void withoutClientCertSslContextHasNoKeyManager() throws Exception {
        var g = TestCerts.generate(dir);
        var m = TlsMaterial.load(Optional.empty(), Optional.empty(), g.caFile());
        assertEquals("", m.clientCertChainPem());
        assertEquals("", m.clientKeyPem());
        assertFalse(m.hasKeyManagers());
        var withKey = TlsMaterial.load(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile());
        assertTrue(withKey.hasKeyManagers());
    }

    @Test
    void sslContextTrustsServerSignedByBundleCa() throws Exception {
        var g = TestCerts.generate(dir, 3, 1);
        var m = TlsMaterial.load(Optional.empty(), Optional.empty(), g.caFile());

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("server", g.serverKey().getPrivate(), new char[0],
                new java.security.cert.Certificate[] {g.serverCert(), g.cas().get(1)});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);

        try (SSLServerSocket server = (SSLServerSocket) serverCtx.getServerSocketFactory().createServerSocket(0)) {
            CountDownLatch done = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                try (SSLSocket s = (SSLSocket) server.accept()) {
                    s.startHandshake();
                    s.getOutputStream().write(1);
                    s.getOutputStream().flush();
                    s.getInputStream().read();
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
            t.setDaemon(true);
            t.start();
            try (SSLSocket c = (SSLSocket) m.sslContext().getSocketFactory().createSocket("localhost", server.getLocalPort())) {
                c.startHandshake();
                assertEquals(1, c.getInputStream().read());
            }
            assertTrue(done.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void holderReloadsWhenFilesChange() throws Exception {
        var g = TestCerts.generate(dir, 3, 1);
        var holder = new TlsMaterialHolder(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile());
        try {
            CountDownLatch called = new CountDownLatch(1);
            holder.addListener(called::countDown);
            assertEquals(3, holder.current().caCertificates().size());
            long before = holder.generation();

            var g2 = TestCerts.generate(dir, 2, 0);
            assertTrue(called.await(5, TimeUnit.SECONDS), "listener not called");
            assertTrue(holder.generation() > before);
            assertEquals(2, holder.current().caCertificates().size());
        } finally {
            holder.close();
        }
    }

    @Test
    void missingCaFileFailsWithPathInMessage() {
        Path missing = dir.resolve("nope-ca.crt");
        var e = assertThrows(IllegalStateException.class,
                () -> TlsMaterial.load(Optional.empty(), Optional.empty(), missing));
        assertTrue(e.getMessage().contains(missing.toString()), e.getMessage());
    }
}
