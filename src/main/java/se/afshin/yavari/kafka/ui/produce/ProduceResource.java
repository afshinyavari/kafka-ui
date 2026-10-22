package se.afshin.yavari.kafka.ui.produce;

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
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.producer.RecordMetadata;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.messages.HeaderView;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistryException;
import se.afshin.yavari.kafka.ui.registry.VersionSummary;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;

@Path("/topics/{name}/produce")
@Produces(MediaType.TEXT_HTML)
public class ProduceResource {

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance form(String name);

        static native TemplateInstance result(String name, RecordMetadata meta);

        static native TemplateInstance schema(String role, SchemaDetector.Detection detection, List<VersionSummary> versions);
    }

    @Inject ProduceService produce;
    @Inject SchemaDetector detector;
    @Inject RegistryClient registry;
    @Inject CredentialsProvider credentials;

    @GET
    public TemplateInstance form(@PathParam("name") String name) {
        return Templates.form(name);
    }

    /** "key" or "value" (the default): which part of the record a schema picker or template is for. */
    public static String role(String role) {
        return "key".equals(role) ? "key" : "value";
    }

    /** A schema picker of the form, preselecting the topic's key or value schema (latest version) when found. */
    @GET
    @Path("schema")
    public TemplateInstance schema(@PathParam("name") String name, @QueryParam("role") String role) {
        String r = role(role);
        var creds = credentials.current();
        var detection = detector.detect(creds, name, "key".equals(r));
        if (detection.isPresent()) {
            try {
                var versions = registry.versions(creds, detection.get().groupId(), detection.get().artifactId())
                        .stream().sorted(Comparator.comparingLong(VersionSummary::globalId).reversed()).toList();
                if (!versions.isEmpty()) {
                    return Templates.schema(r, detection.get(), versions);
                }
            } catch (RegistryException e) {
                // fall through to the plain picker
            }
        }
        return Templates.schema(r, null, List.of());
    }

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance send(
            @PathParam("name") String name,
            @FormParam("partition") String partition,
            @FormParam("key") String key,
            @FormParam("keyMode") String keyMode,
            @FormParam("keyGroup") String keyGroup,
            @FormParam("keyArtifact") String keyArtifact,
            @FormParam("keyVersion") String keyVersion,
            @FormParam("value") String value,
            @FormParam("valueMode") String valueMode,
            @FormParam("valueGroup") String valueGroup,
            @FormParam("valueArtifact") String valueArtifact,
            @FormParam("valueVersion") String valueVersion,
            @FormParam("headerName") List<String> headerNames,
            @FormParam("headerValue") List<String> headerValues) {
        Optional<Integer> part = Optional.empty();
        if (partition != null && !partition.isBlank()) {
            try {
                int number = Integer.parseInt(partition.trim());
                if (number < 0) {
                    throw new NumberFormatException(partition);
                }
                part = Optional.of(number);
            } catch (NumberFormatException e) {
                throw new UserErrorException(new UserError("Invalid message", "Partition must be a non-negative number.", 400));
            }
        }
        List<HeaderView> headers = new ArrayList<>();
        for (int i = 0; headerNames != null && i < headerNames.size(); i++) {
            String v = headerValues != null && i < headerValues.size() ? headerValues.get(i) : "";
            headers.add(new HeaderView(headerNames.get(i), v));
        }
        var meta = produce.produce(credentials.current(), new ProduceRequest(name, part,
                part(key, keyMode, keyGroup, keyArtifact, keyVersion),
                part(value, valueMode, valueGroup, valueArtifact, valueVersion), headers));
        return Templates.result(name, meta);
    }

    private static ProduceRequest.Part part(String text, String mode, String group, String artifact, String version) {
        Optional<SchemaVersionRef> schema = group == null || artifact == null || version == null
                || artifact.isBlank() || version.isBlank()
                ? Optional.empty()
                : Optional.of(new SchemaVersionRef(group, artifact, version));
        return new ProduceRequest.Part(text, "AVRO".equals(mode) ? ProduceMode.AVRO : ProduceMode.RAW, schema);
    }
}
