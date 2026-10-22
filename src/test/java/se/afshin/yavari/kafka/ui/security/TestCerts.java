package se.afshin.yavari.kafka.ui.security;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObjectGenerator;

/** Generates throwaway CAs, client and server certificates for tests. */
public final class TestCerts {

    private TestCerts() {}

    /** Generated material; all keys are PKCS#8 when serialised via {@link #pkcs8Pem}. */
    public record Generated(
            Path caFile,
            Path certFile,
            Path keyFile,
            List<X509Certificate> cas,
            List<KeyPair> caKeys,
            X509Certificate clientCert,
            KeyPair clientKey,
            X509Certificate serverCert,
            KeyPair serverKey) {}

    /**
     * Writes ca.crt (caCount CAs), tls.crt, tls.key (RSA, PKCS#8) into dir. The server cert is
     * signed by CA number serverCaIndex and is valid for localhost / 127.0.0.1.
     */
    public static Generated generate(Path dir, int caCount, int serverCaIndex) {
        try {
            Files.createDirectories(dir);
            List<KeyPair> caKeys = new ArrayList<>();
            List<X509Certificate> cas = new ArrayList<>();
            for (int i = 0; i < caCount; i++) {
                KeyPair kp = rsa();
                cas.add(sign("CN=Test CA " + i, kp, "CN=Test CA " + i, kp, true, false));
                caKeys.add(kp);
            }
            KeyPair clientKey = rsa();
            X509Certificate client =
                    sign("CN=test-client", clientKey, cas.get(0).getSubjectX500Principal().getName(), caKeys.get(0), false, false);
            KeyPair serverKey = rsa();
            X509Certificate server =
                    sign("CN=localhost", serverKey, cas.get(serverCaIndex).getSubjectX500Principal().getName(),
                            caKeys.get(serverCaIndex), false, true);

            Path ca = dir.resolve("ca.crt");
            Path crt = dir.resolve("tls.crt");
            Path key = dir.resolve("tls.key");
            Files.writeString(ca, pem(cas.toArray()));
            Files.writeString(crt, pem(client));
            Files.writeString(key, pkcs8Pem(clientKey.getPrivate()));
            return new Generated(ca, crt, key, cas, caKeys, client, clientKey, server, serverKey);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Generated generate(Path dir) {
        return generate(dir, 3, 1);
    }

    public static KeyPair rsa() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
        g.initialize(2048);
        return g.generateKeyPair();
    }

    public static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(256);
        return g.generateKeyPair();
    }

    /** PEM with a "BEGIN PRIVATE KEY" header (PKCS#8). */
    public static String pkcs8Pem(PrivateKey key) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter w = new JcaPEMWriter(sw)) {
            w.writeObject(new org.bouncycastle.util.io.pem.PemObject("PRIVATE KEY", key.getEncoded()));
        }
        return sw.toString();
    }

    /** PEM with a "BEGIN RSA PRIVATE KEY" header (PKCS#1), for negative tests. */
    public static String pkcs1Pem(PrivateKey rsaKey) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter w = new JcaPEMWriter(sw)) {
            org.bouncycastle.asn1.pkcs.PrivateKeyInfo info =
                    org.bouncycastle.asn1.pkcs.PrivateKeyInfo.getInstance(rsaKey.getEncoded());
            w.writeObject(new org.bouncycastle.util.io.pem.PemObject(
                    "RSA PRIVATE KEY", info.parsePrivateKey().toASN1Primitive().getEncoded()));
        }
        return sw.toString();
    }

    public static String pem(Object... objects) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter w = new JcaPEMWriter(sw)) {
            for (Object o : objects) {
                PemObjectGenerator g = new JcaMiscPEMGenerator(o);
                w.writeObject(g);
            }
        }
        return sw.toString();
    }

    public static X509Certificate sign(
            String subject, KeyPair subjectKey, String issuer, KeyPair issuerKey, boolean ca, boolean server)
            throws Exception {
        Date from = new Date(System.currentTimeMillis() - 60_000);
        Date to = new Date(System.currentTimeMillis() + 24L * 3600_000);
        X509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(
                new X500Name(issuer),
                BigInteger.valueOf(System.nanoTime()),
                from,
                to,
                new X500Name(subject),
                subjectKey.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        if (server) {
            b.addExtension(Extension.subjectAlternativeName, false,
                    new GeneralNames(new GeneralName[] {
                        new GeneralName(GeneralName.dNSName, "localhost"),
                        new GeneralName(GeneralName.iPAddress, "127.0.0.1")
                    }));
        }
        var signer = new JcaContentSignerBuilder("SHA256withRSA").build(issuerKey.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(b.build(signer));
    }
}
