package se.afshin.yavari.kafka.ui.topics;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;
import se.afshin.yavari.kafka.ui.web.Htmx;

@Path("/topics")
@Produces(MediaType.TEXT_HTML)
public class TopicsResource {

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance list(List<TopicRow> topics, String q, boolean internal);

        static native TemplateInstance list$table(List<TopicRow> topics);

        static native TemplateInstance detail(String name, String tab, TopicInfo info);

        static native TemplateInstance panel(String name, String tab, TopicInfo info);
    }

    @Inject TopicService topics;
    @Inject CredentialsProvider credentials;

    @Context HttpHeaders headers;

    @GET
    public TemplateInstance list(@QueryParam("q") String q, @QueryParam("internal") @DefaultValue("false") boolean internal) {
        var rows = topics.list(credentials.current(), q, internal);
        return isFragment() ? Templates.list$table(rows) : Templates.list(rows, q == null ? "" : q, internal);
    }

    @GET
    @Path("{name}")
    public TemplateInstance detail(@PathParam("name") String name) {
        return tab(name, "overview");
    }

    @GET
    @Path("{name}/overview")
    public TemplateInstance overview(@PathParam("name") String name) {
        return tab(name, "overview");
    }

    // {name}/messages is served by MessagesResource, which renders the same panel/detail templates.

    @GET
    @Path("{name}/settings")
    public TemplateInstance settings(@PathParam("name") String name) {
        return tab(name, "settings");
    }

    /** Placeholder until the consumers tab is built. */
    @GET
    @Path("{name}/consumers")
    public TemplateInstance consumers(@PathParam("name") String name) {
        return tab(name, "consumers");
    }

    /** htmx gets the tab bar plus panel (so the active tab updates); otherwise the full page. */
    private TemplateInstance tab(String name, String tab) {
        var info = topics.describe(credentials.current(), name);
        return isFragment() ? Templates.panel(name, tab, info) : Templates.detail(name, tab, info);
    }

    private boolean isFragment() {
        return Htmx.isFragment(headers);
    }
}
