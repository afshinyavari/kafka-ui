package se.afshin.yavari.kafka.ui.registry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import se.afshin.yavari.kafka.ui.security.Credentials;

/**
 * Caches schema lookups per identity. Found is kept 10 minutes, NotFound 1 minute. Denied and Unavailable are only
 * remembered for {@link #FAILURE_TTL} (and never as NotFound): long enough that a scan over many records does not hit
 * the registry for every record, short enough that a permission change or a recovered registry is seen quickly.
 * Bounded to {@link #MAX_ENTRIES} entries (least recently used are evicted).
 */
@ApplicationScoped
public class SchemaCache implements SchemaLookup {

    static final Duration FOUND_TTL = Duration.ofMinutes(10);
    static final Duration NOT_FOUND_TTL = Duration.ofMinutes(1);
    static final Duration FAILURE_TTL = Duration.ofSeconds(10);
    static final int MAX_ENTRIES = 10_000;

    private record Key(String scope, IdKind kind, long id) {}

    private record Entry(Lookup value, Instant expires) {}

    private final RegistryClient client;
    private final Supplier<Instant> clock;
    private final Cache<Key, Entry> entries;

    @Inject
    public SchemaCache(RegistryClient client) {
        this(client, Instant::now);
    }

    SchemaCache(RegistryClient client, Supplier<Instant> clock) {
        this(client, clock, MAX_ENTRIES);
    }

    SchemaCache(RegistryClient client, Supplier<Instant> clock, int maxEntries) {
        this.client = client;
        this.clock = clock;
        // eviction runs on the calling thread, so the bound holds as soon as put returns
        this.entries = Caffeine.newBuilder().maximumSize(maxEntries).executor(Runnable::run).build();
    }

    @Override
    public Lookup get(Credentials credentials, IdKind kind, long id) {
        Key key = new Key(credentials.cacheScope(), kind, id);
        Instant now = clock.get();
        Entry hit = entries.getIfPresent(key);
        if (hit != null && now.isBefore(hit.expires())) {
            return hit.value();
        }
        Lookup result = client.byId(credentials, kind, id);
        Duration ttl = switch (result) {
            case Lookup.Found f -> FOUND_TTL;
            case Lookup.NotFound n -> NOT_FOUND_TTL;
            case Lookup.Denied d -> FAILURE_TTL;
            case Lookup.Unavailable u -> FAILURE_TTL;
        };
        entries.put(key, new Entry(result, now.plus(ttl)));
        return result;
    }

    /** Approximate number of cached entries. */
    long size() {
        entries.cleanUp();
        return entries.estimatedSize();
    }
}
