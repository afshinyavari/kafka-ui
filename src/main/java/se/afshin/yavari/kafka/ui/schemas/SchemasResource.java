package se.afshin.yavari.kafka.ui.schemas;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import se.afshin.yavari.kafka.ui.registry.Page;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.web.Htmx;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.registry.ArtifactSummary;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistryException;
import se.afshin.yavari.kafka.ui.registry.VersionSummary;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;
import se.afshin.yavari.kafka.ui.serde.AvroCodec;
import se.afshin.yavari.kafka.ui.serde.AvroTemplate;

/** Read-only schema registry pages, plus the htmx fragments used by the produce form's schema picker. */
@Path("/schemas")
@Produces(MediaType.TEXT_HTML)
public class SchemasResource {

    private static final int PICKER_LIMIT = 50;
    private static final int PAGE_SIZE = 20;
    private static final ObjectMapper JSON = new ObjectMapper();

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance list(Page<ArtifactSummary> page, String q);

        static native TemplateInstance list$table(Page<ArtifactSummary> page, String q);

        static native TemplateInstance detail(String group, String artifact, List<VersionSummary> versions,
                RegistrySchema schema, String content, String codeClass);

        static native TemplateInstance pickerArtifacts(List<ArtifactSummary> artifacts);

        static native TemplateInstance pickerVersions(String group, String artifact, List<VersionSummary> versions);

        static native TemplateInstance valueField(String value, String skeleton);
    }

    @Inject RegistryClient registry;
    @Inject CredentialsProvider credentials;

    @Context HttpHeaders headers;

    @GET
    public TemplateInstance list(@QueryParam("q") String q, @QueryParam("offset") Integer offset) {
        int off = offset == null || offset < 0 ? 0 : offset;
        Page<ArtifactSummary> page;
        try {
            page = registry.searchArtifacts(credentials.current(), q, off, PAGE_SIZE);
        } catch (RegistryException e) {
            throw new UserErrorException(pageError(e.failure()));
        }
        String query = q == null ? "" : q;
        return Htmx.isFragment(headers) ? Templates.list$table(page, query) : Templates.list(page, query);
    }

    @GET
    @Path("{group}/{artifact}")
    public TemplateInstance detail(
            @PathParam("group") String group,
            @PathParam("artifact") String artifact,
            @QueryParam("version") String version) {
        try {
            var creds = credentials.current();
            var versions = registry.versions(creds, group, artifact).stream()
                    .sorted(java.util.Comparator.comparingLong(VersionSummary::globalId).reversed()).toList();
            if (versions.isEmpty()) {
                throw new UserErrorException(pageError(new Lookup.NotFound()));
            }
            String selected = version == null || version.isBlank() ? versions.get(0).version() : version;
            var schema = registry.version(creds, group, artifact, selected);
            return Templates.detail(group, artifact, versions, schema, pretty(schema.content()),
                    codeClass(schema.type()));
        } catch (RegistryException e) {
            throw new UserErrorException(pageError(e.failure()));
        }
    }

    private static String pretty(String content) {
        try {
            return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(JSON.readTree(content));
        } catch (Exception e) {
            return content;
        }
    }

    private static String codeClass(String type) {
        return "AVRO".equalsIgnoreCase(type) || "JSON".equalsIgnoreCase(type) ? "language-json" : "language-plaintext";
    }

    private static UserError pageError(Lookup failure) {
        if (failure instanceof Lookup.NotFound) {
            return new UserError("Not found", "That schema does not exist in the registry.", 404);
        }
        if (failure instanceof Lookup.Denied) {
            return new UserError("Access denied", "You don't have permission to read this schema.", 403);
        }
        return new UserError("Schema registry unavailable",
                "The schema registry could not be reached. Please try again later.", 502);
    }

    @GET
    @Path("search")
    public TemplateInstance search(@QueryParam("q") String q) {
        try {
            var page = registry.searchArtifacts(credentials.current(), q, 0, PICKER_LIMIT);
            var avro = page.items().stream().filter(a -> "AVRO".equalsIgnoreCase(a.artifactType())).toList();
            return Templates.pickerArtifacts(avro);
        } catch (RegistryException e) {
            throw new UserErrorException(error(e.failure()));
        }
    }

    @GET
    @Path("{group}/{artifact}/versions")
    public TemplateInstance versions(@PathParam("group") String group, @PathParam("artifact") String artifact) {
        try {
            var versions = registry.versions(credentials.current(), group, artifact);
            return Templates.pickerVersions(group, artifact, versions.reversed());
        } catch (RegistryException e) {
            throw new UserErrorException(error(e.failure()));
        }
    }

    /**
     * The produce form's value field, filled with a JSON skeleton of the chosen schema version. Text the user typed
     * or pasted is kept: the field is only replaced while it is empty or still holds the previous skeleton.
     */
    @POST
    @Path("{group}/{artifact}/template")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance template(
            @PathParam("group") String group,
            @PathParam("artifact") String artifact,
            @FormParam("version") String version,
            @FormParam("value") String value,
            @FormParam("valueTemplate") String previous) {
        String current = value == null ? "" : value;
        String template;
        try {
            var creds = credentials.current();
            var schema = registry.version(creds, group, artifact, version);
            if (!"AVRO".equalsIgnoreCase(schema.type())) {
                return Templates.valueField(current, previous == null ? "" : previous);
            }
            template = AvroTemplate.of(AvroCodec.parse(schema,
                    r -> registry.version(creds, r.groupId(), r.artifactId(), r.version()).content()));
        } catch (RuntimeException e) {
            // registry failure or unparseable schema: no skeleton, leave the field as it is
            return Templates.valueField(current, previous == null ? "" : previous);
        }
        boolean untouched = current.isBlank() || normalize(current).equals(normalize(previous));
        return Templates.valueField(untouched ? template : current, template);
    }

    private static String normalize(String s) {
        return s == null ? "" : s.replace("\r\n", "\n").strip();
    }

    private static UserError error(Lookup failure) {
        if (failure instanceof Lookup.NotFound) {
            return new UserError("Not found", "That schema does not exist in the registry.", 404);
        }
        if (failure instanceof Lookup.Denied) {
            return new UserError("Access denied", "You don't have permission to read schemas.", 403);
        }
        return new UserError("Schema registry unavailable", "The schema registry could not be reached.", 502);
    }
}
