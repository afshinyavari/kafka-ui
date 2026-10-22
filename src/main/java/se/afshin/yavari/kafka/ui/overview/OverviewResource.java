package se.afshin.yavari.kafka.ui.overview;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class OverviewResource {

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance dashboard(ClusterOverview cluster);

        static native TemplateInstance brokers(List<BrokerRow> brokers);
    }

    @Inject ClusterService cluster;
    @Inject CredentialsProvider credentials;

    @GET
    public TemplateInstance dashboard() {
        return Templates.dashboard(cluster.overview(credentials.current()));
    }

    @GET
    @Path("brokers")
    public TemplateInstance brokers() {
        return Templates.brokers(cluster.brokers(credentials.current()));
    }
}
