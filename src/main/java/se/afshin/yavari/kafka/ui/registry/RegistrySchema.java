package se.afshin.yavari.kafka.ui.registry;

import java.util.List;

/** A schema version with its content and the references it depends on. */
public record RegistrySchema(
        String type,
        String content,
        String groupId,
        String artifactId,
        String version,
        List<Reference> references) {

    /** A named dependency on another artifact version. */
    public record Reference(String name, String groupId, String artifactId, String version) {}
}
