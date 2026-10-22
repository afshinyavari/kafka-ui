package se.afshin.yavari.kafka.ui.acls;

public record AclRow(String principal, String resourceType, String patternType, String resourceName,
        String operation, String permission, String host) {
}
