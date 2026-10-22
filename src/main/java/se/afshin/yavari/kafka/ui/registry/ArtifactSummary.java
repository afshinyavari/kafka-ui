package se.afshin.yavari.kafka.ui.registry;

import java.time.Instant;

public record ArtifactSummary(String groupId, String artifactId, String name, String artifactType, Instant modifiedOn) {}
