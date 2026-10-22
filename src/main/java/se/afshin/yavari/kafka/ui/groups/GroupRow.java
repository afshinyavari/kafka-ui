package se.afshin.yavari.kafka.ui.groups;

public record GroupRow(String groupId, String state, int members, long totalLag, int topics) {
}
