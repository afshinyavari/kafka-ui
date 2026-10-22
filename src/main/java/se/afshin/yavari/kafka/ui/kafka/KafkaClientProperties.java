package se.afshin.yavari.kafka.ui.kafka;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.Mode;
import se.afshin.yavari.kafka.ui.security.TlsMaterial;

/** Builds Kafka client properties for the configured mode and the given credentials. */
public final class KafkaClientProperties {

    static final String LOGIN_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
    static final String CALLBACK_HANDLER = "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";

    private KafkaClientProperties() {}

    public static Properties forAdmin(KafkaUiConfig cfg, Credentials credentials, TlsMaterial tls) {
        return base(cfg, credentials, tls);
    }

    public static Properties forConsumer(KafkaUiConfig cfg, Credentials credentials, TlsMaterial tls) {
        Properties p = base(cfg, credentials, tls);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public static Properties forProducer(KafkaUiConfig cfg, Credentials credentials, TlsMaterial tls) {
        Properties p = base(cfg, credentials, tls);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return p;
    }

    private static Properties base(KafkaUiConfig cfg, Credentials credentials, TlsMaterial tls) {
        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        p.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, tls.caBundlePem());
        if (cfg.mode() == Mode.MTLS) {
            if (!(credentials instanceof Credentials.Mtls)) {
                throw new IllegalArgumentException("mTLS mode requires Mtls credentials");
            }
            p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            p.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, tls.clientCertChainPem());
            p.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, tls.clientKeyPem());
        } else {
            if (!(credentials instanceof Credentials.Bearer bearer)) {
                throw new IllegalArgumentException("OIDC mode requires Bearer credentials");
            }
            p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            p.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            p.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CALLBACK_HANDLER);
            p.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(bearer.token()));
        }
        return p;
    }

    static String jaas(String token) {
        String escaped = token.replace("\\", "\\\\").replace("\"", "\\\"");
        return LOGIN_MODULE + " required oauth.access.token=\"" + escaped + "\";";
    }
}
