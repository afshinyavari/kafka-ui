package se.afshin.yavari.kafka.ui.produce;

import java.util.List;
import java.util.Optional;
import se.afshin.yavari.kafka.ui.messages.HeaderView;

public record ProduceRequest(
        String topic,
        Optional<Integer> partition,
        String key,
        String value,
        List<HeaderView> headers,
        ProduceMode mode,
        Optional<SchemaVersionRef> valueSchema) {}
