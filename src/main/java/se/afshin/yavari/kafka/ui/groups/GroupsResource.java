package se.afshin.yavari.kafka.ui.groups;

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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;
import se.afshin.yavari.kafka.ui.web.Htmx;

@Path("/consumers")
@Produces(MediaType.TEXT_HTML)
public class GroupsResource {

    /** One line of the reset result. */
    public record ResetLine(String topic, int partition, long offset) {
    }

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance list(List<GroupRow> groups, String q);

        static native TemplateInstance list$table(List<GroupRow> groups);

        static native TemplateInstance detail(GroupDetail group);

        static native TemplateInstance reset(GroupDetail group);

        static native TemplateInstance result(String groupId, List<ResetLine> lines);
    }

    @Inject GroupService groups;
    @Inject ResetOffsetsService resets;
    @Inject CredentialsProvider credentials;

    @Context HttpHeaders headers;

    @GET
    public TemplateInstance list(@QueryParam("q") String q) {
        var rows = groups.list(credentials.current(), q);
        return Htmx.isFragment(headers) ? Templates.list$table(rows) : Templates.list(rows, q == null ? "" : q);
    }

    @GET
    @Path("{id}")
    public TemplateInstance detail(@PathParam("id") String id) {
        return Templates.detail(groups.describe(credentials.current(), id));
    }

    @GET
    @Path("{id}/reset")
    public TemplateInstance resetForm(@PathParam("id") String id) {
        return Templates.reset(groups.describe(credentials.current(), id));
    }

    @POST
    @Path("{id}/reset")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance reset(
            @PathParam("id") String id,
            @FormParam("topic") String topic,
            @FormParam("partitions") String partitions,
            @FormParam("to") String to,
            @FormParam("timestamp") String timestamp,
            @FormParam("offset") String offset) {
        if (topic == null || topic.isBlank()) {
            throw invalid("Pick a topic.");
        }
        ResetTo target;
        try {
            target = ResetTo.valueOf(to == null ? "" : to);
        } catch (IllegalArgumentException e) {
            throw invalid("Pick where to reset the offsets to.");
        }
        var request = new ResetRequest(id, topic, partitionSet(partitions), target,
                target == ResetTo.TIMESTAMP ? Optional.of(parseTimestamp(timestamp)) : Optional.empty(),
                target == ResetTo.OFFSET ? Optional.of(parseOffset(offset)) : Optional.empty());
        Map<TopicPartition, Long> result = resets.reset(credentials.current(), request);
        var lines = result.entrySet().stream()
                .map(e -> new ResetLine(e.getKey().topic(), e.getKey().partition(), e.getValue())).toList();
        return Templates.result(id, lines);
    }

    private static Set<Integer> partitionSet(String text) {
        Set<Integer> result = new LinkedHashSet<>();
        if (text == null || text.isBlank()) {
            return result;
        }
        for (String part : text.split("[,\\s]+")) {
            if (part.isBlank()) {
                continue;
            }
            try {
                int p = Integer.parseInt(part);
                if (p < 0) {
                    throw new NumberFormatException(part);
                }
                result.add(p);
            } catch (NumberFormatException e) {
                throw invalid("Partitions must be non-negative numbers separated by commas.");
            }
        }
        return result;
    }

    /** The form sends a datetime-local value, which is read as UTC. */
    private static Instant parseTimestamp(String text) {
        try {
            return LocalDateTime.parse(text == null ? "" : text.trim()).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw invalid("Pick a valid date and time (UTC).");
        }
    }

    private static long parseOffset(String text) {
        try {
            long o = Long.parseLong(text == null ? "" : text.trim());
            if (o < 0) {
                throw new NumberFormatException(text);
            }
            return o;
        } catch (NumberFormatException e) {
            throw invalid("The offset must be a non-negative number.");
        }
    }

    private static UserErrorException invalid(String message) {
        return new UserErrorException(new UserError("Invalid request", message, 400));
    }
}
