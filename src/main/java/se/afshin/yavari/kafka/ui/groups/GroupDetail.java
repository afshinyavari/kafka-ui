package se.afshin.yavari.kafka.ui.groups;

import java.util.List;

public record GroupDetail(String groupId, String state, List<MemberRow> members, List<OffsetRow> offsets,
        boolean canReset) {

    /** Offsets can only be reset while no consumer is running in the group. */
    public boolean active() {
        return !("EMPTY".equals(state) || "DEAD".equals(state));
    }

    public List<String> topics() {
        return offsets.stream().map(OffsetRow::topic).distinct().toList();
    }
}
