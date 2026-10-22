package se.afshin.yavari.kafka.ui.it;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import se.afshin.yavari.kafka.ui.security.TestCerts;

/**
 * A single KRaft Kafka broker whose client listener is SSL with required client authentication, using certificates
 * from {@link TestCerts}: the broker's certificate is signed by test CA 1 and it trusts only test CA 0, which signed
 * the UI's client certificate ({@code CN=test-client}). ACLs are enforced by the StandardAuthorizer; the UI's
 * principal and the internal PLAINTEXT listeners (ANONYMOUS) are super users.
 */
public class SslKafkaResource implements QuarkusTestResourceLifecycleManager {

    static final DockerImageName IMAGE = DockerImageName.parse("apache/kafka:4.2.2");
    static final String UI_PRINCIPAL = "User:CN=test-client";
    private static final String SECRETS = "/etc/kafka/secrets/";
    private static final String PASSWORD = "changeit";

    private GenericContainer<?> kafka;

    @Override
    public Map<String, String> start() {
        try {
            Path dir = Path.of("target", "it-ssl-kafka");
            TestCerts.Generated certs = TestCerts.generate(dir, 3, 1);
            Path keystore = dir.resolve("server.p12");
            Path truststore = dir.resolve("trust.p12");
            Path password = dir.resolve("password");
            writeKeystore(keystore, certs);
            writeTruststore(truststore, certs);
            Files.writeString(password, PASSWORD);

            // advertised SSL port must be the host port, so bind the same number on both sides
            int port = freePort();
            kafka = new GenericContainer<>(IMAGE)
                    .withExposedPorts(port)
                    .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))))
                    .withCopyFileToContainer(MountableFile.forHostPath(keystore, 0644), SECRETS + "server.p12")
                    .withCopyFileToContainer(MountableFile.forHostPath(truststore, 0644), SECRETS + "trust.p12")
                    .withCopyFileToContainer(MountableFile.forHostPath(password, 0644), SECRETS + "password")
                    .withEnv(Map.ofEntries(
                            Map.entry("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw"),
                            Map.entry("KAFKA_NODE_ID", "1"),
                            Map.entry("KAFKA_PROCESS_ROLES", "broker,controller"),
                            Map.entry("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
                            Map.entry("KAFKA_LISTENERS", "SSL://0.0.0.0:" + port
                                    + ",BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"),
                            Map.entry("KAFKA_ADVERTISED_LISTENERS", "SSL://localhost:" + port + ",BROKER://localhost:9092"),
                            Map.entry("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                    "SSL:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                            Map.entry("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
                            Map.entry("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                            Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
                            Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
                            Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
                            Map.entry("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
                            // the image's configure script resolves these to ssl.keystore.* / ssl.truststore.*
                            Map.entry("KAFKA_SSL_KEYSTORE_FILENAME", "server.p12"),
                            Map.entry("KAFKA_SSL_KEYSTORE_CREDENTIALS", "password"),
                            Map.entry("KAFKA_SSL_KEY_CREDENTIALS", "password"),
                            Map.entry("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12"),
                            Map.entry("KAFKA_SSL_TRUSTSTORE_FILENAME", "trust.p12"),
                            Map.entry("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "password"),
                            Map.entry("KAFKA_SSL_TRUSTSTORE_TYPE", "PKCS12"),
                            Map.entry("KAFKA_SSL_CLIENT_AUTH", "required"),
                            Map.entry("KAFKA_AUTHORIZER_CLASS_NAME",
                                    "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
                            Map.entry("KAFKA_SUPER_USERS", UI_PRINCIPAL + ";User:ANONYMOUS")))
                    .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(2));
            kafka.start();
            return Map.of(
                    "kafka-ui.mode", "mtls",
                    "kafka-ui.bootstrap-servers", "localhost:" + port,
                    "kafka-ui.tls.cert-file", certs.certFile().toString(),
                    "kafka-ui.tls.key-file", certs.keyFile().toString(),
                    "kafka-ui.tls.ca-file", certs.caFile().toString());
        } catch (Exception e) {
            throw new IllegalStateException("Cannot start the SSL Kafka broker: " + e.getMessage(), e);
        }
    }

    private static void writeKeystore(Path file, TestCerts.Generated certs) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("server", certs.serverKey().getPrivate(), PASSWORD.toCharArray(),
                new Certificate[] {certs.serverCert(), certs.cas().get(1)});
        store(ks, file);
    }

    private static void writeTruststore(Path file, TestCerts.Generated certs) throws Exception {
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("client-ca", certs.cas().get(0));
        store(ts, file);
    }

    private static void store(KeyStore ks, Path file) throws Exception {
        try (OutputStream out = Files.newOutputStream(file)) {
            ks.store(out, PASSWORD.toCharArray());
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Override
    public void stop() {
        if (kafka != null) {
            kafka.stop();
        }
    }
}
