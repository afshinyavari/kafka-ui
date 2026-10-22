package se.afshin.yavari.kafka.ui.serde;

import se.afshin.yavari.kafka.ui.registry.IdKind;

/** A reference to a registry schema and where on the wire it was found. */
public record SchemaRef(IdKind kind, long id, Source source) {

    /** Where the schema id was read from. */
    public enum Source {
        HEADER,
        PAYLOAD_4,
        PAYLOAD_8
    }
}
