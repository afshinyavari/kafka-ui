package se.afshin.yavari.kafka.ui.serde;

import java.util.Optional;

/** A decoded key or value, with the reason the format was chosen and the schema reference used, if any. */
public record Decoded(Format format, String text, String explanation, Optional<SchemaRef> ref) {}
