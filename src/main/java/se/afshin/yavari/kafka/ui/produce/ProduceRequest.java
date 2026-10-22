package se.afshin.yavari.kafka.ui.produce;

import java.util.List;
import java.util.Optional;
import se.afshin.yavari.kafka.ui.messages.HeaderView;

public record ProduceRequest(
        String topic,
        Optional<Integer> partition,
        Part key,
        Part value,
        List<HeaderView> headers) {

    /** The key or the value: raw text, or JSON to encode with an Avro schema version. */
    public record Part(String text, ProduceMode mode, Optional<SchemaVersionRef> schema) {

        public static Part raw(String text) {
            return new Part(text, ProduceMode.RAW, Optional.empty());
        }
    }
}
