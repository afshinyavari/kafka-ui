package se.afshin.yavari.kafka.ui.groups;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/** @param partitions empty means every partition of the topic that has a committed offset */
public record ResetRequest(String groupId, String topic, Set<Integer> partitions, ResetTo to,
        Optional<Instant> timestamp, Optional<Long> offset) {
}
