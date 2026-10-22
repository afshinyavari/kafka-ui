package se.afshin.yavari.kafka.ui.topics;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;

@Path("/topics")
@Produces(MediaType.TEXT_HTML)
public class TopicsResource {

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance list(List<TopicRow> topics, String q, boolean internal);

        static native TemplateInstance list$table(List<TopicRow> topics);

        static native TemplateInstance detail(String name, String tab, TopicInfo info);

        static native TemplateInstance overview(String name, TopicInfo info);

        static native TemplateInstance settings(String name, TopicInfo info);

        static native TemplateInstance messages(String name, TopicInfo info);

        static native TemplateInstance consumers(String name, TopicInfo info);
    }

    @Inject TopicService topics;
    @Inject CredentialsProvider credentials;

    @GET
    public TemplateInstance list(@QueryParam("q") String q, @QueryParam("internal") @DefaultValue("false") boolean internal,
            @HeaderParam("HX-Request") String htmx) {
        var rows = topics.list(credentials.current(), q, internal);
        return isHtmx(htmx) ? Templates.list$table(rows) : Templates.list(rows, q == null ? "" : q, internal);
    }

    @GET
    @Path("{name}")
    public TemplateInstance detail(@PathParam("name") String name) {
        return Templates.detail(name, "overview", topics.describe(credentials.current(), name));
    }

    @GET
    @Path("{name}/overview")
    public TemplateInstance overview(@PathParam("name") String name, @HeaderParam("HX-Request") String htmx) {
        var info = topics.describe(credentials.current(), name);
        return isHtmx(htmx) ? Templates.overview(name, info) : Templates.detail(name, "overview", info);
    }

    @GET
    @Path("{name}/settings")
    public TemplateInstance settings(@PathParam("name") String name, @HeaderParam("HX-Request") String htmx) {
        var info = topics.describe(credentials.current(), name);
        return isHtmx(htmx) ? Templates.settings(name, info) : Templates.detail(name, "settings", info);
    }

    /** Placeholder until the messages tab is built. */
    @GET
    @Path("{name}/messages")
    public TemplateInstance messages(@PathParam("name") String name, @HeaderParam("HX-Request") String htmx) {
        var info = topics.describe(credentials.current(), name);
        return isHtmx(htmx) ? Templates.messages(name, info) : Templates.detail(name, "messages", info);
    }

    /** Placeholder until the consumers tab is built. */
    @GET
    @Path("{name}/consumers")
    public TemplateInstance consumers(@PathParam("name") String name, @HeaderParam("HX-Request") String htmx) {
        var info = topics.describe(credentials.current(), name);
        return isHtmx(htmx) ? Templates.consumers(name, info) : Templates.detail(name, "consumers", info);
    }

    private static boolean isHtmx(String header) {
        return "true".equalsIgnoreCase(header);
    }
}
