package se.afshin.yavari.kafka.ui.serde;

/** How a key or value was decoded for display. */
public enum Format {
    NULL,
    EMPTY,
    AVRO,
    JSON_SCHEMA,
    JSON,
    XML,
    TEXT,
    HEX
}
