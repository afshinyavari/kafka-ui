package se.afshin.yavari.kafka.ui.groups;

public record OffsetRow(String topic, int partition, long committed, long end, long lag) {
}
