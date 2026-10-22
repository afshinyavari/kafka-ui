package se.afshin.yavari.kafka.ui.schemas;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.registry.ArtifactSummary;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistryException;
import se.afshin.yavari.kafka.ui.registry.VersionSummary;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;

/** Schema registry routes. For now only the htmx fragments used by the produce form's schema picker. */
@Path("/schemas")
@Produces(MediaType.TEXT_HTML)
public class SchemasResource {

    private static final int PICKER_LIMIT = 50;

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance pickerArtifacts(List<ArtifactSummary> artifacts);

        static native TemplateInstance pickerVersions(String group, String artifact, List<VersionSummary> versions);
    }

    @Inject RegistryClient registry;
    @Inject CredentialsProvider credentials;

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
