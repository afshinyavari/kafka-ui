package se.afshin.yavari.kafka.ui.registry;

import se.afshin.yavari.kafka.ui.security.Credentials;

/** Resolves a schema id; implemented by {@link SchemaCache} and faked in unit tests. */
@FunctionalInterface
public interface SchemaLookup {
    Lookup get(Credentials credentials, IdKind kind, long id);
}
