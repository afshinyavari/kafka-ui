package se.afshin.yavari.kafka.ui.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.Mode;
import se.afshin.yavari.kafka.ui.security.TestCerts;
import se.afshin.yavari.kafka.ui.security.TlsMaterial;

class KafkaClientPropertiesTest {

    @TempDir Path dir;
    TlsMaterial tls;
    final Instant exp = Instant.now().plusSeconds(300);

    @BeforeEach
    void setUp() {
        var g = TestCerts.generate(dir);
        tls = TlsMaterial.load(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile());
    }

    static KafkaUiConfig cfg(Mode mode) {
        return new KafkaUiConfig() {
            public Mode mode() { return mode; }
            public String bootstrapServers() { return "broker:9093"; }
            public Tls tls() { throw new UnsupportedOperationException(); }
            public Registry registry() { throw new UnsupportedOperationException(); }
            public Search search() { throw new UnsupportedOperationException(); }
            public Tail tail() { throw new UnsupportedOperationException(); }
            public Produce produce() { throw new UnsupportedOperationException(); }
        };
    }

    @Test
    void consumersStartFromEarliestWhenTheirPositionIsLost() {
        Properties p = KafkaClientProperties.forConsumer(cfg(Mode.MTLS), new Credentials.Mtls(), tls);
        assertThat(p).containsEntry("auto.offset.reset", "earliest").containsEntry("enable.auto.commit", "false");
    }

    @Test
    void consumersNeverAutoCreateTopics() {
        Properties p = KafkaClientProperties.forConsumer(cfg(Mode.MTLS), new Credentials.Mtls(), tls);
        assertThat(p).containsEntry("allow.auto.create.topics", "false");
    }

    @Test
    void mtlsUsesSslWithInlinePemAndNoSasl() {
        Properties p = KafkaClientProperties.forAdmin(cfg(Mode.MTLS), new Credentials.Mtls(), tls);
        assertThat(p).containsEntry("security.protocol", "SSL")
                .containsEntry("bootstrap.servers", "broker:9093")
                .containsEntry("ssl.keystore.type", "PEM").containsEntry("ssl.truststore.type", "PEM")
                .containsKey("ssl.keystore.certificate.chain").containsKey("ssl.keystore.key")
                .containsKey("ssl.truststore.certificates").doesNotContainKey("sasl.mechanism");
    }

    @Test
    void adminHasBoundedTimeouts() {
        Properties p = KafkaClientProperties.forAdmin(cfg(Mode.MTLS), new Credentials.Mtls(), tls);
        assertThat(p).containsEntry("default.api.timeout.ms", "15000").containsEntry("request.timeout.ms", "10000");
        assertThat(KafkaClientProperties.forConsumer(cfg(Mode.MTLS), new Credentials.Mtls(), tls))
                .doesNotContainKey("default.api.timeout.ms");
    }

    @Test
    void oidcUsesSaslSslOauthBearerWithUserToken() {
        Properties p = KafkaClientProperties.forAdmin(cfg(Mode.OIDC), new Credentials.Bearer("tok.en.x", "alice", exp), tls);
        assertThat(p).containsEntry("security.protocol", "SASL_SSL").containsEntry("sasl.mechanism", "OAUTHBEARER")
                .containsEntry("sasl.login.callback.handler.class",
                        "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler")
                .containsEntry("ssl.truststore.type", "PEM")
                .doesNotContainKey("ssl.keystore.key").doesNotContainKey("ssl.keystore.type");
        assertThat(p.getProperty("sasl.jaas.config")).contains("oauth.access.token=\"tok.en.x\"")
                .startsWith("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required");
    }

    @Test
    void jaasEscapesQuotesAndBackslashes() {
        assertThat(KafkaClientProperties.jaas("a\"b\\c")).contains("oauth.access.token=\"a\\\"b\\\\c\";");
    }

    @Test
    void consumerNeverCommitsAndHasNoGroup() {
        Properties p = KafkaClientProperties.forConsumer(cfg(Mode.MTLS), new Credentials.Mtls(), tls);
        assertThat(p).containsEntry("enable.auto.commit", "false").doesNotContainKey("group.id")
                .containsEntry("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .containsEntry("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    @Test
    void producerUsesByteArraySerializers() {
        Properties p = KafkaClientProperties.forProducer(cfg(Mode.MTLS), new Credentials.Mtls(), tls);
        assertThat(p).containsEntry("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
                .containsEntry("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @Test
    void cacheScopes() {
        assertThat(new Credentials.Mtls().cacheScope()).isEqualTo("mtls");
        assertThat(new Credentials.Bearer("t", "alice", exp).cacheScope()).isEqualTo("user:alice");
    }
}
