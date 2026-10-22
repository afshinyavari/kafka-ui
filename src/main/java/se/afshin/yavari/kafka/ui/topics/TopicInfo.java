package se.afshin.yavari.kafka.ui.topics;

import java.util.List;
import java.util.Map;

/**
 * A topic's partitions, settings and what the caller may do. {@code configs} is only loaded for the Settings tab;
 * {@code settingsDenied} is the message shown there when Kafka refused to describe the topic's configuration.
 */
public record TopicInfo(String name, List<PartitionRow> partitions, Map<String, ConfigRow> configs,
        boolean canProduce, String settingsDenied) {

    public TopicInfo(String name, List<PartitionRow> partitions, Map<String, ConfigRow> configs, boolean canProduce) {
        this(name, partitions, configs, canProduce, null);
    }
}
