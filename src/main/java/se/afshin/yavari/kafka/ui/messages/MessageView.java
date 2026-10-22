package se.afshin.yavari.kafka.ui.messages;

import java.time.Instant;
import java.util.List;
import se.afshin.yavari.kafka.ui.serde.Decoded;
import se.afshin.yavari.kafka.ui.serde.Format;

/** A decoded record as shown in result rows and on the detail page. */
public record MessageView(int partition, long offset, Instant timestamp, Decoded key, Decoded value,
        List<HeaderView> headers) {

    static final int PREVIEW_LENGTH = 200;

    public String keyPreview() {
        return preview(key);
    }

    public String valuePreview() {
        return preview(value);
    }

    /** Single-line, truncated text for result rows; the detail page shows everything. */
    static String preview(Decoded d) {
        String oneLine = d.text().replaceAll("\\s+", " ").strip();
        if (oneLine.isEmpty()) {
            return d.format() == Format.NULL ? "null" : "";
        }
        return oneLine.length() <= PREVIEW_LENGTH ? oneLine : oneLine.substring(0, PREVIEW_LENGTH) + "…";
    }
}
