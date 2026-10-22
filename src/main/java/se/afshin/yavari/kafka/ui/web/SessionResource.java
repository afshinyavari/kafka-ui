package se.afshin.yavari.kafka.ui.web;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Cheap authenticated endpoint the page calls after a stream ended because the access token expired: going through
 * the normal request path lets Quarkus OIDC refresh the session; an unrefreshable session is redirected to login.
 */
@Path("/session/ping")
public class SessionResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String ping() {
        return "ok";
    }
}
