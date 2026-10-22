package se.afshin.yavari.kafka.ui.acls;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.SecurityDisabledException;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** Read-only ACL listing. */
@ApplicationScoped
public class AclService {

    public static final String DISABLED_MESSAGE = "ACLs are not enabled on this cluster.";

    @Inject KafkaClients clients;

    public List<AclRow> list(Credentials credentials, AclFilter filter) {
        try {
            return filter(fetch(credentials), filter);
        } catch (Exception e) {
            Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof SecurityDisabledException) {
                throw new UserErrorException(new UserError("ACLs not enabled", DISABLED_MESSAGE, 200));
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new UserErrorException(KafkaErrors.translate(e, "the ACLs"));
        }
    }

    /** Seam for tests. */
    Collection<AclBinding> fetch(Credentials credentials) throws Exception {
        Admin admin = clients.admin(credentials);
        return admin.describeAcls(AclBindingFilter.ANY).values().get();
    }

    static List<AclRow> filter(Collection<AclBinding> bindings, AclFilter f) {
        String principal = lower(f.principal());
        String name = lower(f.name());
        String type = blank(f.type()) ? null : f.type().trim();
        String op = blank(f.operation()) ? null : f.operation().trim();
        return bindings.stream()
                .map(b -> new AclRow(
                        b.entry().principal(),
                        b.pattern().resourceType().name(),
                        b.pattern().patternType().name(),
                        b.pattern().name(),
                        b.entry().operation().name(),
                        b.entry().permissionType().name(),
                        b.entry().host()))
                .filter(r -> principal.isEmpty() || r.principal().toLowerCase(Locale.ROOT).contains(principal))
                .filter(r -> name.isEmpty() || r.resourceName().toLowerCase(Locale.ROOT).contains(name))
                .filter(r -> type == null || r.resourceType().equalsIgnoreCase(type))
                .filter(r -> op == null || r.operation().equalsIgnoreCase(op))
                .sorted(Comparator.comparing(AclRow::principal).thenComparing(AclRow::resourceType)
                        .thenComparing(AclRow::resourceName).thenComparing(AclRow::operation))
                .toList();
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static String lower(String s) {
        return blank(s) ? "" : s.trim().toLowerCase(Locale.ROOT);
    }
}
