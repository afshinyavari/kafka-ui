package se.afshin.yavari.kafka.ui.security;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Client certificate, PKCS#8 key and CA bundle read from PEM files. */
public final class TlsMaterial {

    private static final char[] NO_PASSWORD = new char[0];

    private final String clientCertChainPem;
    private final String clientKeyPem;
    private final String caBundlePem;
    private final List<X509Certificate> caCertificates;
    private final SSLContext sslContext;
    private final boolean keyManagers;

    private TlsMaterial(
            String clientCertChainPem,
            String clientKeyPem,
            String caBundlePem,
            List<X509Certificate> caCertificates,
            SSLContext sslContext,
            boolean keyManagers) {
        this.clientCertChainPem = clientCertChainPem;
        this.clientKeyPem = clientKeyPem;
        this.caBundlePem = caBundlePem;
        this.caCertificates = caCertificates;
        this.sslContext = sslContext;
        this.keyManagers = keyManagers;
    }

    public static TlsMaterial load(Optional<Path> cert, Optional<Path> key, Path caBundle) {
        if (cert.isPresent() != key.isPresent()) {
            throw new IllegalStateException("TLS client certificate and key must be configured together (cert="
                    + cert.map(Path::toString).orElse("<none>") + ", key="
                    + key.map(Path::toString).orElse("<none>") + ")");
        }
        String caPem = read(caBundle);
        List<X509Certificate> cas = certificates(caPem, caBundle);
        if (cas.isEmpty()) {
            throw new IllegalStateException("No certificates found in CA bundle " + caBundle);
        }
        String certPem = "";
        String keyPem = "";
        List<X509Certificate> chain = List.of();
        PrivateKey privateKey = null;
        if (cert.isPresent()) {
            certPem = read(cert.get());
            chain = certificates(certPem, cert.get());
            if (chain.isEmpty()) {
                throw new IllegalStateException("No certificates found in " + cert.get());
            }
            keyPem = read(key.get());
            privateKey = privateKey(keyPem, key.get());
        }
        try {
            KeyStore trust = KeyStore.getInstance("PKCS12");
            trust.load(null, null);
            for (int i = 0; i < cas.size(); i++) {
                trust.setCertificateEntry("ca-" + i, cas.get(i));
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trust);

            KeyManager[] kms = null;
            if (privateKey != null) {
                KeyStore ks = KeyStore.getInstance("PKCS12");
                ks.load(null, null);
                ks.setKeyEntry("client", privateKey, NO_PASSWORD, chain.toArray(new Certificate[0]));
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, NO_PASSWORD);
                kms = kmf.getKeyManagers();
            }
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kms, tmf.getTrustManagers(), null);
            return new TlsMaterial(certPem, keyPem, caPem, List.copyOf(cas), ctx, kms != null);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException("Cannot build SSLContext from TLS files: " + e.getMessage(), e);
        }
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    public String clientCertChainPem() {
        return clientCertChainPem;
    }

    /** PKCS#8 PEM exactly as read. */
    public String clientKeyPem() {
        return clientKeyPem;
    }

    public String caBundlePem() {
        return caBundlePem;
    }

    public List<X509Certificate> caCertificates() {
        return caCertificates;
    }

    boolean hasKeyManagers() {
        return keyManagers;
    }

    private static String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read TLS file " + path + ": " + e.getMessage(), e);
        }
    }

    private static List<X509Certificate> certificates(String pem, Path path) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return cf.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))).stream()
                    .map(X509Certificate.class::cast)
                    .toList();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot parse certificates in " + path + ": " + e.getMessage(), e);
        }
    }

    private static PrivateKey privateKey(String pem, Path path) {
        String begin = "-----BEGIN PRIVATE KEY-----";
        String end = "-----END PRIVATE KEY-----";
        int b = pem.indexOf(begin);
        int e = pem.indexOf(end);
        if (b < 0 || e < b) {
            throw new IllegalStateException(
                    "Private key " + path + " must be an unencrypted PKCS#8 PEM (BEGIN PRIVATE KEY)");
        }
        byte[] der;
        try {
            der = Base64.getMimeDecoder().decode(pem.substring(b + begin.length(), e));
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException("Private key " + path + " is not valid PKCS#8 PEM: " + ex.getMessage(), ex);
        }
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        for (String alg : List.of("RSA", "EC")) {
            try {
                return KeyFactory.getInstance(alg).generatePrivate(spec);
            } catch (GeneralSecurityException ignored) {
                // try next algorithm
            }
        }
        throw new IllegalStateException("Private key " + path + " is not a PKCS#8 RSA or EC key");
    }
}
