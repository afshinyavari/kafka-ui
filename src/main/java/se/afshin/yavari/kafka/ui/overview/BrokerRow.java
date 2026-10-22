package se.afshin.yavari.kafka.ui.overview;

public record BrokerRow(int id, String address, String rack, int leaderPartitions) {
}
