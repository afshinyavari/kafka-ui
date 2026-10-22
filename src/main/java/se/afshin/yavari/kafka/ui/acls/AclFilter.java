package se.afshin.yavari.kafka.ui.acls;

/** In-memory ACL filter; blank fields match everything. */
public record AclFilter(String principal, String name, String type, String operation) {

    public static final AclFilter NONE = new AclFilter(null, null, null, null);
}
