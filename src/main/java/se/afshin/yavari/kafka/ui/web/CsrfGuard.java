package se.afshin.yavari.kafka.ui.web;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Locale;
import java.util.Set;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

/**
 * Rejects cross-site state-changing requests: every method except GET/HEAD/OPTIONS needs the {@code HX-Request}
 * header (which browsers will not send cross-site without a CORS preflight) and, when the browser says where the
 * request came from (Origin, else Referer), that origin must be this host.
 */
public class CsrfGuard {
    private static final Logger LOG = Logger.getLogger(CsrfGuard.class);
    private static final Set<String> SAFE = Set.of("GET", "HEAD", "OPTIONS");

    @ServerRequestFilter
    public Response check(ContainerRequestContext request) {
        if (SAFE.contains(request.getMethod().toUpperCase(Locale.ROOT))) {
            return null;
        }
        if (!"true".equalsIgnoreCase(request.getHeaderString("HX-Request"))
                || !sameOrigin(request.getHeaderString("Host"), request.getHeaderString("Origin"),
                        request.getHeaderString("Referer"))) {
            LOG.warnf("Blocked %s %s: missing HX-Request header or foreign origin", request.getMethod(),
                    request.getUriInfo().getPath());
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.TEXT_HTML)
                    .entity("<div class=\"error\" role=\"alert\"><h2>Request blocked</h2>"
                            + "<p>Request blocked: cross-site request</p></div>")
                    .build();
        }
        return null;
    }

    static boolean sameOrigin(String host, String origin, String referer) {
        String source = origin != null && !origin.isBlank() ? origin : referer;
        if (source == null || source.isBlank()) {
            return true;
        }
        if (host == null) {
            return false;
        }
        try {
            URI uri = URI.create(source.trim());
            if (uri.getHost() == null) {
                return false;
            }
            return authority(uri.getHost(), uri.getPort(), uri.getScheme())
                    .equals(hostAuthority(host.trim(), uri.getScheme()));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String hostAuthority(String host, String scheme) {
        int colon = host.lastIndexOf(':');
        if (colon > 0 && host.indexOf(']') < colon) {
            try {
                return authority(host.substring(0, colon), Integer.parseInt(host.substring(colon + 1)), scheme);
            } catch (NumberFormatException e) {
                return host.toLowerCase(Locale.ROOT);
            }
        }
        return authority(host, -1, scheme);
    }

    private static String authority(String host, int port, String scheme) {
        int effective = port != -1 ? port : "https".equalsIgnoreCase(scheme) ? 443 : 80;
        return host.toLowerCase(Locale.ROOT) + ":" + effective;
    }
}
