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

        static native TemplateInstance schema(SchemaDetector.Detection detection, List<VersionSummary> versions);
    }

    @Inject ProduceService produce;
    @Inject SchemaDetector detector;
    @Inject RegistryClient registry;
    @Inject CredentialsProvider credentials;

    @GET
    public TemplateInstance form(@PathParam("name") String name) {
        return Templates.form(name);
    }

    /** The form's schema picker, preselecting the topic's schema (latest version) when one can be found. */
    @GET
    @Path("schema")
    public TemplateInstance schema(@PathParam("name") String name) {
        var creds = credentials.current();
        var detection = detector.detect(creds, name);
        if (detection.isPresent()) {
            try {
                var versions = registry.versions(creds, detection.get().groupId(), detection.get().artifactId())
                        .stream().sorted(Comparator.comparingLong(VersionSummary::globalId).reversed()).toList();
                if (!versions.isEmpty()) {
                    return Templates.schema(detection.get(), versions);
                }
            } catch (RegistryException e) {
                // fall through to the plain picker
            }
        }
        return Templates.schema(null, List.of());
    }

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance send(
            @PathParam("name") String name,
            @FormParam("mode") String mode,
            @FormParam("partition") String partition,
            @FormParam("key") String key,
            @FormParam("value") String value,
            @FormParam("headerName") List<String> headerNames,
            @FormParam("headerValue") List<String> headerValues,
            @FormParam("group") String group,
            @FormParam("artifact") String artifact,
            @FormParam("version") String version) {
        ProduceMode produceMode = "AVRO".equals(mode) ? ProduceMode.AVRO : ProduceMode.RAW;
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
        Optional<SchemaVersionRef> schema = group == null || artifact == null || version == null
                || artifact.isBlank() || version.isBlank()
                ? Optional.empty()
                : Optional.of(new SchemaVersionRef(group, artifact, version));
        var meta = produce.produce(credentials.current(),
                new ProduceRequest(name, part, key, value, headers, produceMode, schema));
        return Templates.result(name, meta);
    }
}
