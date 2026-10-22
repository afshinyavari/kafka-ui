package se.afshin.yavari.kafka.ui.web;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;

/** Test-only resource that throws, to exercise UserErrorMapper. */
@Path("/test-errors")
@Produces(MediaType.TEXT_HTML)
public class ThrowingResource {
    @GET
    @Path("user")
    public String user() {
        throw new UserErrorException(new UserError("Access denied",
                "You don't have permission to access topic orders.", 403));
    }

    @GET
    @Path("boom")
    public String boom() {
        throw new IllegalStateException("secret-detail");
    }
}
