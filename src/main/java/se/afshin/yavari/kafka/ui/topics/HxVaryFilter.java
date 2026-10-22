package se.afshin.yavari.kafka.ui.topics;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

/** Topic pages answer differently to htmx requests, so caches must vary on the header. */
@Provider
public class HxVaryFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) {
        if (request.getUriInfo().getPath().startsWith("/topics")) {
            response.getHeaders().putSingle("Vary", "HX-Request, HX-History-Restore-Request");
        }
    }
}
