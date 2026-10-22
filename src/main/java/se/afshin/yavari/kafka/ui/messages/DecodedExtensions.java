package se.afshin.yavari.kafka.ui.messages;

import io.quarkus.qute.TemplateExtension;
import se.afshin.yavari.kafka.ui.serde.Decoded;
import se.afshin.yavari.kafka.ui.serde.Format;

/** Template helpers for decoded keys and values. */
@TemplateExtension
public final class DecodedExtensions {

    private DecodedExtensions() {
    }

    /** highlight.js class for the decoded text. */
    static String codeClass(Decoded d) {
        return switch (d.format()) {
            case AVRO, JSON_SCHEMA, JSON -> "language-json";
            case XML -> "language-xml";
            default -> "nohighlight";
        };
    }

    static String label(Format f) {
        return switch (f) {
            case AVRO -> "Avro";
            case JSON_SCHEMA -> "JSON Schema";
            case JSON -> "JSON";
            case XML -> "XML";
            case TEXT -> "Text";
            case HEX -> "Hex";
            case NULL -> "Null";
            case EMPTY -> "Empty";
        };
    }
}
