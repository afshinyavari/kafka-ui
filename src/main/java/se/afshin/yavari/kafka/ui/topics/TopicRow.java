package se.afshin.yavari.kafka.ui.topics;

public record TopicRow(String name, int partitions, int replicationFactor, Long messageCount, boolean internal) {
}
