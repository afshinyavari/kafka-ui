package se.afshin.yavari.kafka.ui.web;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;

/**
 * Cheap authenticated endpoint the page calls after a stream ended because the access token expired: going through
 * the normal request path lets Quarkus OIDC refresh the session; an unrefreshable session is redirected to login.
 * {@code X-Token-Expires-In} reports the access token's remaining lifetime in seconds so the page can tell whether
 * the refresh actually happened; it is absent when the credentials never expire (mTLS) or no token is available.
 */
@Path("/session/ping")
public class SessionResource {

    public static final String EXPIRES_IN = "X-Token-Expires-In";

    @Inject CredentialsProvider credentials;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response ping() {
        Response.ResponseBuilder ok = Response.ok("ok");
        try {
            credentials.current().remaining(Instant.now())
                    .ifPresent(d -> ok.header(EXPIRES_IN, Math.max(0, d.toSeconds())));
        } catch (IllegalStateException e) {
            // no OIDC token on this identity: nothing to report
        }
        return ok.build();
    }
}
