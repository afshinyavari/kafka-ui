package se.afshin.yavari.kafka.ui.web;

import io.quarkus.arc.Arc;
import io.quarkus.qute.TemplateGlobal;
import io.quarkus.security.identity.SecurityIdentity;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Mode;

/** Qute global data available in every template. */
@TemplateGlobal
public final class Layout {
    static final String MTLS_BANNER = "mTLS mode – no login, all users share the UI certificate's permissions";

    private Layout() {
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
}
