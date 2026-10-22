package se.afshin.yavari.kafka.ui.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;

/**
 * Read-only client for the Apicurio Registry v3 REST API. Forwards the user's bearer token, or uses the client
 * certificate (mTLS). The token is never logged.
 */
@ApplicationScoped
public class RegistryClient {

    private static final Logger LOG = Logger.getLogger(RegistryClient.class);
    private static final String API_PATH = "/apis/registry/v3";
    private static final String DEFAULT_GROUP = "default";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String base;
    private final TlsMaterialHolder tls;

    private volatile HttpClient http;
    private volatile long httpGeneration = -1;

    @Inject
    public RegistryClient(KafkaUiConfig config, TlsMaterialHolder tls) {
        this(config.registry().url(), tls);
    }

    /** {@code url} is the registry URL, with or without the {@code /apis/registry/v3} suffix. */
    public RegistryClient(URI url, TlsMaterialHolder tls) {
        String u = url.toString();
        while (u.endsWith("/")) {
            u = u.substring(0, u.length() - 1);
        }
        this.base = u.endsWith(API_PATH) ? u : u + API_PATH;
        this.tls = tls;
    }

    public Lookup byId(Credentials credentials, IdKind kind, long id) {
        String idPath = "/ids/" + (kind == IdKind.GLOBAL_ID ? "globalIds/" : "contentIds/") + id;
        String searchParam = (kind == IdKind.GLOBAL_ID ? "globalId=" : "contentId=") + id;
        try {
            String content = body(credentials, idPath);
            JsonNode meta = JSON.readTree(body(credentials, "/search/versions?" + searchParam)).path("versions");
            JsonNode first = meta.isArray() && !meta.isEmpty() ? meta.get(0) : null;
            List<RegistrySchema.Reference> refs = references(optionalBody(credentials, idPath + "/references"));
            return new Lookup.Found(new RegistrySchema(
                    text(first, "artifactType"),
                    content,
                    first == null ? null : groupOf(first),
                    text(first, "artifactId"),
                    text(first, "version"),
                    refs));
        } catch (FailedLookup f) {
            return f.failure;
        } catch (IOException e) {
            return new Lookup.Unavailable("Unreadable registry response: " + e.getMessage());
        }
    }

    public Page<ArtifactSummary> searchArtifacts(Credentials credentials, String nameFilter, int offset, int limit) {
        StringBuilder path = new StringBuilder("/search/artifacts?offset=" + offset + "&limit=" + limit);
        if (nameFilter != null && !nameFilter.isBlank()) {
            path.append("&name=").append(encode(nameFilter));
        }
        try {
            JsonNode root = JSON.readTree(body(credentials, path.toString()));
            List<ArtifactSummary> items = new ArrayList<>();
            for (JsonNode a : root.path("artifacts")) {
                items.add(new ArtifactSummary(
                        groupOf(a),
                        text(a, "artifactId"),
                        text(a, "name"),
                        text(a, "artifactType"),
                        instant(a, "modifiedOn")));
            }
            return new Page<>(items, root.path("count").asLong(items.size()), offset, limit);
        } catch (FailedLookup f) {
            throw new RegistryException(f.failure);
        } catch (IOException e) {
            throw new RegistryException(new Lookup.Unavailable("Unreadable registry response: " + e.getMessage()));
        }
    }

    public List<VersionSummary> versions(Credentials credentials, String groupId, String artifactId) {
        try {
            JsonNode root = JSON.readTree(body(credentials, artifactPath(groupId, artifactId) + "/versions"));
            List<VersionSummary> out = new ArrayList<>();
            for (JsonNode v : root.path("versions")) {
                out.add(new VersionSummary(
                        text(v, "version"),
                        v.path("globalId").asLong(),
                        v.path("contentId").asLong(),
                        instant(v, "createdOn"),
                        text(v, "state")));
            }
            return out;
        } catch (FailedLookup f) {
            throw new RegistryException(f.failure);
        } catch (IOException e) {
            throw new RegistryException(new Lookup.Unavailable("Unreadable registry response: " + e.getMessage()));
        }
    }

    public RegistrySchema version(Credentials credentials, String groupId, String artifactId, String version) {
        String path = artifactPath(groupId, artifactId) + "/versions/" + encode(version);
        try {
            String content = body(credentials, path + "/content");
            JsonNode meta = JSON.readTree(body(credentials, path));
            List<RegistrySchema.Reference> refs = references(optionalBody(credentials, path + "/references"));
            return new RegistrySchema(
                    text(meta, "artifactType"),
                    content,
                    groupId == null ? DEFAULT_GROUP : groupId,
                    artifactId,
                    version,
                    refs);
        } catch (FailedLookup f) {
            throw new RegistryException(f.failure);
        } catch (IOException e) {
            throw new RegistryException(new Lookup.Unavailable("Unreadable registry response: " + e.getMessage()));
        }
    }

    // ---- HTTP plumbing ----

    /** Thrown internally to carry a non-success outcome out of the helper chain. */
    private static final class FailedLookup extends RuntimeException {
        final transient Lookup failure;

        FailedLookup(Lookup failure) {
            super(null, null, false, false);
            this.failure = failure;
        }
    }

    private String body(Credentials credentials, String path) {
        return send(credentials, path).body();
    }

    /** Like body(), but a 404 means "none" (empty JSON array). */
    private String optionalBody(Credentials credentials, String path) {
        try {
            return body(credentials, path);
        } catch (FailedLookup f) {
            if (f.failure instanceof Lookup.NotFound) {
                return "[]";
            }
            throw f;
        }
    }

    private HttpResponse<String> send(Credentials credentials, String path) {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/json, */*")
                .GET();
        if (credentials instanceof Credentials.Bearer bearer) {
            req.header("Authorization", "Bearer " + bearer.token());
        }
        HttpResponse<String> response;
        try {
            response = client().send(req.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.debugf("Registry request %s failed: %s", path, e.toString());
            throw new FailedLookup(new Lookup.Unavailable(
                    "Cannot reach the schema registry: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FailedLookup(new Lookup.Unavailable("Interrupted while calling the schema registry"));
        }
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return response;
        }
        LOG.debugf("Registry request %s -> HTTP %d", path, status);
        if (status == 404) {
            throw new FailedLookup(new Lookup.NotFound());
        }
        if (status == 401 || status == 403) {
            throw new FailedLookup(new Lookup.Denied(status));
        }
        throw new FailedLookup(new Lookup.Unavailable("Schema registry answered HTTP " + status));
    }

    private HttpClient client() {
        long generation = tls.generation();
        HttpClient c = http;
        if (c != null && httpGeneration == generation) {
            return c;
        }
        synchronized (this) {
            generation = tls.generation();
            if (http == null || httpGeneration != generation) {
                http = HttpClient.newBuilder()
                        .sslContext(tls.current().sslContext())
                        .connectTimeout(Duration.ofSeconds(5))
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build();
                httpGeneration = generation;
            }
            return http;
        }
    }

    // ---- JSON helpers ----

    private static List<RegistrySchema.Reference> references(String json) throws IOException {
        List<RegistrySchema.Reference> out = new ArrayList<>();
        for (JsonNode r : JSON.readTree(json)) {
            out.add(new RegistrySchema.Reference(
                    text(r, "name"), groupOf(r), text(r, "artifactId"), text(r, "version")));
        }
        return List.copyOf(out);
    }

    private static String groupOf(JsonNode n) {
        String g = text(n, "groupId");
        return g == null ? DEFAULT_GROUP : g;
    }

    private static String text(JsonNode n, String field) {
        if (n == null) {
            return null;
        }
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static Instant instant(JsonNode n, String field) {
        String s = text(n, field);
        if (s == null) {
            return null;
        }
        try {
            return Instant.parse(s);
        } catch (java.time.format.DateTimeParseException e) {
            return null;
        }
    }

    private static String artifactPath(String groupId, String artifactId) {
        return "/groups/" + encode(groupId == null ? DEFAULT_GROUP : groupId) + "/artifacts/" + encode(artifactId);
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
