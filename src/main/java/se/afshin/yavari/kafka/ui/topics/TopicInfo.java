package se.afshin.yavari.kafka.ui.topics;

import java.util.List;
import java.util.Map;

public record TopicInfo(String name, List<PartitionRow> partitions, Map<String, ConfigRow> configs,
        boolean canProduce) {
}
