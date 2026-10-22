package se.afshin.yavari.kafka.ui.web;

import jakarta.ws.rs.core.HttpHeaders;

/** htmx request helpers. */
public final class Htmx {

    private Htmx() {
    }

    /** htmx wants a fragment; history restores need the full page even though they carry HX-Request. */
    public static boolean isFragment(HttpHeaders headers) {
        return "true".equalsIgnoreCase(headers.getHeaderString("HX-Request"))
                && !"true".equalsIgnoreCase(headers.getHeaderString("HX-History-Restore-Request"));
    }
}
