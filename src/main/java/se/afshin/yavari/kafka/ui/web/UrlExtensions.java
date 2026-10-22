package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.TemplateExtension;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/** Qute helpers. */
@TemplateExtension
public final class UrlExtensions {

    private UrlExtensions() {
    }

    /** Percent-encodes a value for use as a URL path segment or query value. */
    public static String urlEnc(String s) {
        return s == null ? "" : URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
