package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;

/** Renders friendly errors: a full page normally, just the error box for htmx requests. */
@Provider
public class UserErrorMapper implements ExceptionMapper<Throwable> {
    private static final Logger LOG = Logger.getLogger(UserErrorMapper.class);

    @Inject
    @Location("fragments/error")
    Template error;

    @Context
    HttpHeaders headers;

    @Override
    public Response toResponse(Throwable t) {
        if (t instanceof WebApplicationException wae) {
            return wae.getResponse();
        }
        if (t instanceof io.quarkus.security.ForbiddenException) {
            return Response.status(403).build();
        }
        if (t instanceof io.quarkus.security.UnauthorizedException
                || t instanceof io.quarkus.security.AuthenticationFailedException) {
            return Response.status(401).build();
        }
        UserError userError;
        if (t instanceof UserErrorException e) {
            userError = e.error();
        } else {
            userError = KafkaErrors.translate(t, "the requested resource");
            if (userError.httpStatus() == 500) {
                LOG.error("Unexpected error while handling request", t);
            } else {
                LOG.warnf("Request failed: %s (%s)", userError.message(), t.toString());
            }
        }
        boolean htmx = "true".equalsIgnoreCase(headers.getHeaderString("HX-Request"));
        TemplateInstance instance = (htmx ? error.getFragment("box").instance() : error.instance()).data("error", userError);
        return Response.status(userError.httpStatus()).type(MediaType.TEXT_HTML).entity(instance.render()).build();
    }
}
