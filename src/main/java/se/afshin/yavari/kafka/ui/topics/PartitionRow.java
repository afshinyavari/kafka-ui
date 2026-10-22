package se.afshin.yavari.kafka.ui.topics;

import java.util.List;

/** Offsets are null when the broker did not report them. */
public record PartitionRow(int partition, int leader, List<Integer> replicas, List<Integer> isr,
        Long startOffset, Long endOffset) {

    public Long messages() {
        return startOffset == null || endOffset == null ? null : endOffset - startOffset;
    }
}
