package se.afshin.yavari.kafka.ui.registry;

/** A registry call that is not a schema-id lookup failed; {@link #failure()} says how. */
public class RegistryException extends RuntimeException {

    private final Lookup failure;

    public RegistryException(Lookup failure) {
        super("Schema registry request failed: " + failure);
        this.failure = failure;
    }

    /** NotFound, Denied or Unavailable. */
    public Lookup failure() {
        return failure;
    }
}
