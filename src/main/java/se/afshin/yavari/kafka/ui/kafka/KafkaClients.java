package se.afshin.yavari.kafka.ui.kafka;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;

/** Creates Kafka clients per credential. Admin clients are cached; consumers and producers are not. */
@ApplicationScoped
public class KafkaClients {

    private static final Logger LOG = Logger.getLogger(KafkaClients.class);
    private static final Duration IDLE = Duration.ofMinutes(5);

    record Key(String scope, int tokenHash, long tlsGeneration) {}

    record Entry(Admin admin, Instant expiresAt) {}

    private final KafkaUiConfig config;
    private final TlsMaterialHolder tls;
    private final KafkaPropertiesCustomizer customizer;
    private final LoadingCache<CacheRequest, Entry> admins;

    private record CacheRequest(Key key, Credentials credentials) {
        // identity is the key only; credentials ride along for loading
        @Override
        public boolean equals(Object o) {
            return o instanceof CacheRequest r && r.key.equals(key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }

    @Inject
    public KafkaClients(KafkaUiConfig config, TlsMaterialHolder tls, KafkaPropertiesCustomizer customizer) {
        this.config = config;
        this.tls = tls;
        this.customizer = customizer;
        this.admins = Caffeine.newBuilder()
                .executor(Runnable::run)
                .expireAfter(new Expiry<CacheRequest, Entry>() {
                    @Override
                    public long expireAfterCreate(CacheRequest k, Entry v, long now) {
                        return ttl(v);
                    }

                    @Override
                    public long expireAfterUpdate(CacheRequest k, Entry v, long now, long current) {
                        return ttl(v);
                    }

                    @Override
                    public long expireAfterRead(CacheRequest k, Entry v, long now, long current) {
                        return Math.min(ttl(v), IDLE.toNanos());
                    }
                })
                .removalListener((CacheRequest k, Entry v, com.github.benmanes.caffeine.cache.RemovalCause c) -> {
                    if (v != null) {
                        close(v.admin());
                    }
                })
                .build(r -> new Entry(createAdmin(r.credentials()), expiry(r.credentials())));
        tls.addListener(admins::invalidateAll);
    }

    public Admin admin(Credentials credentials) {
        Entry e = admins.get(new CacheRequest(key(credentials), credentials));
        return e.admin();
    }

    /** Caller closes. */
    public KafkaConsumer<byte[], byte[]> newConsumer(Credentials credentials) {
        return new KafkaConsumer<>(customizer.customize(
                KafkaClientProperties.forConsumer(config, credentials, tls.current())));
    }

    public KafkaProducer<byte[], byte[]> producer(Credentials credentials) {
        return new KafkaProducer<>(customizer.customize(
                KafkaClientProperties.forProducer(config, credentials, tls.current())));
    }

    protected Admin createAdmin(Credentials credentials) {
        Properties p = customizer.customize(KafkaClientProperties.forAdmin(config, credentials, tls.current()));
        return Admin.create(p);
    }

    private Key key(Credentials credentials) {
        int tokenHash = credentials instanceof Credentials.Bearer b ? b.token().hashCode() : 0;
        return new Key(credentials.cacheScope(), tokenHash, tls.generation());
    }

    private static Instant expiry(Credentials credentials) {
        return credentials instanceof Credentials.Bearer b ? b.expiresAt() : null;
    }

    private static long ttl(Entry e) {
        long idle = IDLE.toNanos();
        if (e.expiresAt() == null) {
            return idle;
        }
        long untilExpiry = Duration.between(Instant.now(), e.expiresAt()).toNanos();
        return Math.max(0, Math.min(idle, untilExpiry));
    }

    private static void close(Admin admin) {
        try {
            admin.close(Duration.ofSeconds(5));
        } catch (RuntimeException e) {
            LOG.debug("Closing cached Admin client failed", e);
        }
    }

    @PreDestroy
    void shutdown() {
        admins.invalidateAll();
    }
}
