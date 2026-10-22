package se.afshin.yavari.kafka.ui.overview;

public record ClusterOverview(String clusterId, String controller, int brokers, int topics, int partitions,
        int underReplicatedPartitions) {
}
