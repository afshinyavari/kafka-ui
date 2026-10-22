package se.afshin.yavari.kafka.ui.web;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Content version of the static assets, appended as {@code ?v=} to their URLs. Quarkus serves static resources as
 * {@code immutable}, so the URL must change whenever the content does.
 */
final class AssetVersion {
    private static final String ROOT = "META-INF/resources/";
    static final List<String> ASSETS = List.of("css/app.css", "vendor/github.min.css", "vendor/github-dark.min.css",
            "vendor/htmx.min.js", "vendor/htmx-ext-sse.js", "vendor/highlight.min.js");
    private static final String VERSION = compute();

    private AssetVersion() {
    }

    static String current() {
        return VERSION;
    }

    /** First 10 hex chars of SHA-256 over the given contents (length-prefixed so boundaries matter). */
    static String hash(byte[]... contents) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (byte[] c : contents) {
                md.update(Integer.toString(c.length).getBytes());
                md.update((byte) ':');
                md.update(c);
            }
            return HexFormat.of().formatHex(md.digest()).substring(0, 10);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String compute() {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl == null) {
                cl = AssetVersion.class.getClassLoader();
            }
            List<byte[]> all = new ArrayList<>();
            for (String asset : ASSETS) {
                try (InputStream in = cl.getResourceAsStream(ROOT + asset)) {
                    if (in == null) {
                        return startupFallback();
                    }
                    all.add(in.readAllBytes());
                }
            }
            return hash(all.toArray(new byte[0][]));
        } catch (IOException | RuntimeException e) {
            return startupFallback();
        }
    }

    private static String startupFallback() {
        return Long.toString(System.currentTimeMillis(), 36);
    }
}
