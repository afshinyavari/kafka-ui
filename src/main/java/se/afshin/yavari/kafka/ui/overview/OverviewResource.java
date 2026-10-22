package se.afshin.yavari.kafka.ui.overview;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/")
public class OverviewResource {

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance dashboard();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance dashboard() {
        return Templates.dashboard();
    }
}
