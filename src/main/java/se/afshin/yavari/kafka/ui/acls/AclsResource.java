package se.afshin.yavari.kafka.ui.acls;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;
import se.afshin.yavari.kafka.ui.web.Htmx;

/** Read-only ACL page. */
@Path("/acls")
@Produces(MediaType.TEXT_HTML)
public class AclsResource {

    static final List<String> TYPES = Arrays.stream(ResourceType.values())
            .filter(t -> t != ResourceType.UNKNOWN && t != ResourceType.ANY).map(Enum::name).toList();
    static final List<String> OPERATIONS = Arrays.stream(AclOperation.values())
            .filter(o -> o != AclOperation.UNKNOWN && o != AclOperation.ANY).map(Enum::name).toList();

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance list(List<AclRow> acls, AclFilter filter, List<String> types,
                List<String> operations);

        static native TemplateInstance list$table(List<AclRow> acls);
    }

    @Inject AclService acls;
    @Inject CredentialsProvider credentials;

    @Context HttpHeaders headers;

    @GET
    public TemplateInstance list(
            @QueryParam("principal") String principal,
            @QueryParam("name") String name,
            @QueryParam("type") String type,
            @QueryParam("operation") String operation) {
        var filter = new AclFilter(principal == null ? "" : principal, name == null ? "" : name,
                type == null ? "" : type, operation == null ? "" : operation);
        var rows = acls.list(credentials.current(), filter);
        return Htmx.isFragment(headers)
                ? Templates.list$table(rows)
                : Templates.list(rows, filter, TYPES, OPERATIONS);
    }
}
