package se.afshin.yavari.kafka.ui.registry;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import se.afshin.yavari.kafka.ui.security.Credentials;

/**
 * Caches schema lookups per identity. Found is kept 10 minutes, NotFound 1 minute; Denied and Unavailable are
 * never cached, so a permission change or an outage is seen on the next call.
 */
@ApplicationScoped
public class SchemaCache {

    static final Duration FOUND_TTL = Duration.ofMinutes(10);
    static final Duration NOT_FOUND_TTL = Duration.ofMinutes(1);
    private static final int MAX_ENTRIES = 10_000;

    private record Key(String scope, IdKind kind, long id) {}

    private record Entry(Lookup value, Instant expires) {}

    private final RegistryClient client;
    private final Supplier<Instant> clock;
    private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();

    @Inject
    public SchemaCache(RegistryClient client) {
        this(client, Instant::now);
    }

    SchemaCache(RegistryClient client, Supplier<Instant> clock) {
        this.client = client;
        this.clock = clock;
    }

    public Lookup get(Credentials credentials, IdKind kind, long id) {
        Key key = new Key(credentials.cacheScope(), kind, id);
        Instant now = clock.get();
        Entry hit = entries.get(key);
        if (hit != null && now.isBefore(hit.expires())) {
            return hit.value();
        }
        Lookup result = client.byId(credentials, kind, id);
        Duration ttl = switch (result) {
            case Lookup.Found f -> FOUND_TTL;
            case Lookup.NotFound n -> NOT_FOUND_TTL;
            case Lookup.Denied d -> null;
            case Lookup.Unavailable u -> null;
        };
        if (ttl == null) {
            entries.remove(key);
        } else {
            if (entries.size() >= MAX_ENTRIES) {
                entries.values().removeIf(e -> !now.isBefore(e.expires()));
                if (entries.size() >= MAX_ENTRIES) {
                    entries.clear();
                }
            }
            entries.put(key, new Entry(result, now.plus(ttl)));
        }
        return result;
    }
}
