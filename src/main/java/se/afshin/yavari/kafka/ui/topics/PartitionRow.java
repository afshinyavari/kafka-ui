package se.afshin.yavari.kafka.ui.topics;

import java.util.List;

public record PartitionRow(int partition, int leader, List<Integer> replicas, List<Integer> isr,
        long startOffset, long endOffset) {
}
