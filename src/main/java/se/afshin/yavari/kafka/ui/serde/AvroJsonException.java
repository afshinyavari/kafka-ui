package se.afshin.yavari.kafka.ui.serde;

/** JSON could not be converted to Avro; {@code fieldPath} names the offending field when known (else empty). */
public class AvroJsonException extends RuntimeException {

    private final String fieldPath;

    public AvroJsonException(String fieldPath, String message) {
        super(message);
        this.fieldPath = fieldPath;
    }

    public String fieldPath() {
        return fieldPath;
    }
}
