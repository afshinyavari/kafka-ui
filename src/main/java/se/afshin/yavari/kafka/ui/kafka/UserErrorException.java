package se.afshin.yavari.kafka.ui.kafka;

/** Thrown by resources to show a friendly error page or fragment. */
public class UserErrorException extends RuntimeException {
    private final transient UserError error;

    public UserErrorException(UserError error) {
        super(error.message());
        this.error = error;
    }

    public UserError error() {
        return error;
    }
}
