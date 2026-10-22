package se.afshin.yavari.kafka.ui.web;

import io.quarkus.arc.Arc;
import io.quarkus.qute.TemplateGlobal;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.vertx.http.runtime.CurrentVertxRequest;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Mode;

/** Qute global data available in every template. */
@TemplateGlobal
public final class Layout {
    static final String MTLS_BANNER = "mTLS mode – no login, all users share the UI certificate's permissions";

    private Layout() {
    }

    /** Content version for static asset URLs (cache busting). */
    public static String assetVersion() {
        return AssetVersion.current();
    }

    public static Mode mode() {
        return Arc.container().instance(KafkaUiConfig.class).get().mode();
    }

    /** Name of the logged-in user (OIDC mode), otherwise null. */
    public static String username() {
        if (mode() != Mode.OIDC) {
            return null;
        }
        var identity = Arc.container().instance(SecurityIdentity.class).get();
        return identity.isAnonymous() ? null : identity.getPrincipal().getName();
    }

    /** Permanent banner text (mTLS mode), otherwise null. */
    public static String bannerText() {
        return mode() == Mode.MTLS ? MTLS_BANNER : null;
    }

    /** Sidebar entry for the current request path ("dashboard", "topics", ...), or "" when none matches. */
    public static String navSection() {
        if (!Arc.container().requestContext().isActive()) {
            return "";
        }
        var current = Arc.container().instance(CurrentVertxRequest.class).get().getCurrent();
        return current == null ? "" : section(current.request().path());
    }

    private static String section(String path) {
        if (path == null || path.equals("/")) {
            return "dashboard";
        }
        for (String s : new String[] {"brokers", "topics", "consumers", "schemas", "acls"}) {
            if (path.equals("/" + s) || path.startsWith("/" + s + "/")) {
                return s;
            }
        }
        return "";
    }
}
