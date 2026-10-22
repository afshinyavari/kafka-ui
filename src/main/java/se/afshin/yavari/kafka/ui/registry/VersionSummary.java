package se.afshin.yavari.kafka.ui.registry;

import java.time.Instant;

public record VersionSummary(String version, long globalId, long contentId, Instant createdOn, String state) {}
