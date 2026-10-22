package se.afshin.yavari.kafka.ui.registry;

/** Outcome of resolving a schema id in the registry. */
public sealed interface Lookup permits Lookup.Found, Lookup.NotFound, Lookup.Denied, Lookup.Unavailable {

    record Found(RegistrySchema schema) implements Lookup {}

    record NotFound() implements Lookup {}

    /** The registry (or its proxy) answered 401 or 403. */
    record Denied(int status) implements Lookup {}

    /** Connection error, timeout or server error. */
    record Unavailable(String reason) implements Lookup {}
}
