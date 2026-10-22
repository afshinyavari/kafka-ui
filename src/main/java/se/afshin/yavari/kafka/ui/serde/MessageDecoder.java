package se.afshin.yavari.kafka.ui.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.registry.SchemaLookup;
import se.afshin.yavari.kafka.ui.security.Credentials;

/**
 * Decodes record keys and values automatically and explains the choice. Order: tombstone/empty, schema reference
 * from headers, schema reference from the payload prefix, then content sniffing (JSON, XML, text, hex).
 * A decoding failure never escapes; it ends up in the explanation.
 */
@ApplicationScoped
public class MessageDecoder {

    /** Fetches the content of a referenced schema version. */
    @FunctionalInterface
    public interface ReferenceLoader {
        String content(Credentials credentials, RegistrySchema.Reference reference);
    }

    static final int HEX_LIMIT = 4096;
    private static final int PARSED_CACHE_LIMIT = 500;

    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final SchemaLookup schemas;
    private final ReferenceLoader references;
    /**
     * Parsed Avro schemas keyed by the registry schema value. Safe across identities: every decode still goes through
     * the (identity-scoped) lookup first, so the cache never grants access to a schema.
     */
    private final ConcurrentHashMap<RegistrySchema, Schema> parsed = new ConcurrentHashMap<>();

    @Inject
    public MessageDecoder(se.afshin.yavari.kafka.ui.registry.SchemaCache schemas, RegistryClient client) {
        this(schemas, (c, r) -> client.version(c, r.groupId(), r.artifactId(), r.version()).content());
    }

    public MessageDecoder(SchemaLookup schemas, ReferenceLoader references) {
        this.schemas = schemas;
        this.references = references;
    }

    public Decoded decode(Credentials credentials, byte[] data, Headers headers, boolean isKey) {
        return decodeAs(credentials, data, headers, isKey, null);
    }

    /** {@code forced == null} means automatic. AVRO / JSON_SCHEMA forced still need a schema reference. */
    public Decoded decodeAs(Credentials credentials, byte[] data, Headers headers, boolean isKey, Format forced) {
        if (data == null) {
            return new Decoded(Format.NULL, "", "Tombstone (null)", Optional.empty());
        }
        if (data.length == 0) {
            return new Decoded(Format.EMPTY, "", "Empty (0 bytes)", Optional.empty());
        }
        Headers h = headers == null ? new RecordHeaders() : headers;
        try {
            if (forced == null || forced == Format.AVRO || forced == Format.JSON_SCHEMA) {
                return withRegistry(credentials, data, h, isKey, forced);
            }
            return manual(data, forced);
        } catch (RuntimeException e) {
            return sniff(data, List.of("Decoding failed: " + message(e)));
        }
    }

    private Decoded manual(byte[] data, Format forced) {
        String why = " (chosen manually)";
        switch (forced) {
            case HEX:
                return new Decoded(Format.HEX, hexdump(data), "Shown as hex" + why, Optional.empty());
            case JSON: {
                Optional<String> json = prettyJson(utf8(data), true);
                if (json.isPresent()) {
                    return new Decoded(Format.JSON, json.get(), "Shown as JSON" + why, Optional.empty());
                }
                return sniff(data, List.of("Not valid JSON"));
            }
            case XML: {
                Optional<String> xml = XmlPretty.pretty(utf8(data));
                if (xml.isPresent()) {
                    return new Decoded(Format.XML, xml.get(), "Shown as XML" + why, Optional.empty());
                }
                return sniff(data, List.of("Not valid XML"));
            }
            default:
                return new Decoded(Format.TEXT, new String(data, StandardCharsets.UTF_8),
                        "Shown as text" + why + (decodeUtf8(data, 0) == null ? " – invalid UTF-8 replaced" : ""),
                        Optional.empty());
        }
    }

    private Decoded withRegistry(Credentials credentials, byte[] data, Headers headers, boolean isKey, Format forced) {
        List<String> notes = new ArrayList<>();
        Optional<SchemaRef> headerRef = WireFormat.fromHeaders(headers, isKey);
        if (headerRef.isPresent()) {
            SchemaRef ref = headerRef.get();
            boolean jsonEncoded = WireFormat.encodingHeader(headers, isKey)
                    .map(e -> e.equalsIgnoreCase("JSON")).orElse(false);
            Decoded d = attempt(credentials, ref, data, 0, jsonEncoded, forced, notes);
            if (d != null) {
                return d;
            }
        } else {
            for (WireFormat.Candidate c : WireFormat.payloadCandidates(data)) {
                Decoded d = attempt(credentials, c.ref(), data, c.bodyOffset(), false, forced, notes);
                if (d != null) {
                    return d;
                }
            }
        }
        if (notes.isEmpty()) {
            notes.add("No schema reference");
        }
        if (forced == Format.AVRO || forced == Format.JSON_SCHEMA) {
            notes.add(0, "Cannot show as " + (forced == Format.AVRO ? "Avro" : "JSON Schema") + " (chosen manually)");
        }
        return sniff(data, notes);
    }

    /** Returns a decoded result, or null after adding the reason to {@code notes}. */
    private Decoded attempt(Credentials credentials, SchemaRef ref, byte[] data, int offset, boolean jsonEncoded,
            Format forced, List<String> notes) {
        String idText = idLabel(ref);
        Lookup lookup;
        try {
            lookup = schemas.get(credentials, ref.kind(), ref.id());
        } catch (RuntimeException e) {
            notes.add("Schema registry unavailable");
            return null;
        }
        switch (lookup) {
            case Lookup.NotFound n -> {
                notes.add("No schema found for id " + ref.id() + " (" + sourceLabel(ref) + ")");
                return null;
            }
            case Lookup.Denied d -> {
                notes.add("No permission to read schema " + kindLabel(ref.kind()) + " " + ref.id());
                return null;
            }
            case Lookup.Unavailable u -> {
                notes.add("Schema registry unavailable");
                return null;
            }
            case Lookup.Found f -> {
                RegistrySchema s = f.schema();
                String type = s.type() == null ? "" : s.type().toUpperCase();
                String where = "schema " + s.groupId() + "/" + s.artifactId() + " v" + s.version() + ", " + idText;
                switch (type) {
                    case "AVRO" -> {
                        if (forced == Format.JSON_SCHEMA) {
                            notes.add("Schema " + s.groupId() + "/" + s.artifactId() + " is Avro, not JSON Schema");
                            return null;
                        }
                        try {
                            Schema schema = parsed(credentials, s);
                            byte[] body = java.util.Arrays.copyOfRange(data, offset, data.length);
                            String json = prettyOrRaw(AvroCodec.toJson(schema, body, jsonEncoded));
                            return new Decoded(Format.AVRO, json, "Avro – " + where + " " + origin(ref),
                                    Optional.of(ref));
                        } catch (RuntimeException | LinkageError e) {
                            notes.add("Avro decoding failed (" + where + " " + origin(ref) + "): " + message(e));
                            return null;
                        }
                    }
                    case "JSON" -> {
                        if (forced == Format.AVRO) {
                            notes.add("Schema " + s.groupId() + "/" + s.artifactId() + " is JSON Schema, not Avro");
                            return null;
                        }
                        String text = decodeUtf8(data, offset);
                        Optional<String> json = text == null ? Optional.empty() : prettyJson(text, false);
                        if (json.isEmpty()) {
                            notes.add("JSON Schema decoding failed (" + where + " " + origin(ref)
                                    + "): body is not valid JSON");
                            return null;
                        }
                        return new Decoded(Format.JSON_SCHEMA, json.get(), "JSON Schema – " + where + " " + origin(ref),
                                Optional.of(ref));
                    }
                    default -> {
                        notes.add("Unsupported schema type " + type);
                        return null;
                    }
                }
            }
        }
    }

    private Schema parsed(Credentials credentials, RegistrySchema s) {
        Schema cached = parsed.get(s);
        if (cached != null) {
            return cached;
        }
        Schema schema = AvroCodec.parse(s, r -> references.content(credentials, r));
        if (parsed.size() >= PARSED_CACHE_LIMIT) {
            parsed.clear();
        }
        parsed.put(s, schema);
        return schema;
    }

    private Decoded sniff(byte[] data, List<String> notes) {
        String prefix = String.join("; ", notes) + " – detected ";
        String text = decodeUtf8(data, 0);
        if (text == null || hasBinaryControls(text)) {
            return new Decoded(Format.HEX, hexdump(data), prefix + "binary", Optional.empty());
        }
        Optional<String> json = prettyJson(text, true);
        if (json.isPresent()) {
            return new Decoded(Format.JSON, json.get(), prefix + "JSON", Optional.empty());
        }
        Optional<String> xml = XmlPretty.pretty(text);
        if (xml.isPresent()) {
            return new Decoded(Format.XML, xml.get(), prefix + "XML", Optional.empty());
        }
        return new Decoded(Format.TEXT, text, prefix + "text", Optional.empty());
    }

    private static String idLabel(SchemaRef ref) {
        return kindLabel(ref.kind()) + " " + ref.id();
    }

    private static String kindLabel(IdKind kind) {
        return kind == IdKind.GLOBAL_ID ? "globalId" : "contentId";
    }

    private static String sourceLabel(SchemaRef ref) {
        return switch (ref.source()) {
            case HEADER -> kindLabel(ref.kind()) + " from header";
            case PAYLOAD_4 -> "4-byte contentId";
            case PAYLOAD_8 -> "8-byte globalId";
        };
    }

    private static String origin(SchemaRef ref) {
        return switch (ref.source()) {
            case HEADER -> "from header";
            case PAYLOAD_4 -> "from payload (4-byte contentId)";
            case PAYLOAD_8 -> "from payload (8-byte globalId)";
        };
    }

    private static String message(Throwable e) {
        String m = e.getMessage();
        return m == null || m.isBlank() ? e.getClass().getSimpleName() : m;
    }

    private static String utf8(byte[] data) {
        String t = decodeUtf8(data, 0);
        return t == null ? "" : t;
    }

    /** Strict UTF-8 decode of data from offset, or null when invalid. */
    private static String decodeUtf8(byte[] data, int offset) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data, offset, data.length - offset))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static boolean hasBinaryControls(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
                return true;
            }
        }
        return false;
    }

    /** Pretty JSON; with {@code containerOnly} bare scalars (numbers, strings) are not treated as JSON. */
    private static Optional<String> prettyJson(String text, boolean containerOnly) {
        try {
            JsonNode node = JSON.readTree(text);
            if (node == null || node.isMissingNode() || (containerOnly && !node.isContainerNode())) {
                return Optional.empty();
            }
            return Optional.of(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static String prettyOrRaw(String json) {
        return prettyJson(json, false).orElse(json);
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    static String hexdump(byte[] data) {
        int n = Math.min(data.length, HEX_LIMIT);
        StringBuilder sb = new StringBuilder(n * 4 + 128);
        for (int off = 0; off < n; off += 16) {
            for (int shift = 28; shift >= 0; shift -= 4) {
                sb.append(HEX[(off >>> shift) & 0xf]);
            }
            sb.append("  ");
            for (int i = 0; i < 16; i++) {
                if (off + i < n) {
                    int b = data[off + i] & 0xff;
                    sb.append(HEX[b >>> 4]).append(HEX[b & 0xf]).append(' ');
                } else {
                    sb.append("   ");
                }
                if (i == 7) {
                    sb.append(' ');
                }
            }
            sb.append(" |");
            for (int i = off; i < Math.min(off + 16, n); i++) {
                int b = data[i] & 0xff;
                sb.append(b >= 0x20 && b < 0x7f ? (char) b : '.');
            }
            sb.append("|\n");
        }
        if (data.length > n) {
            sb.append("… truncated, showing first ").append(n).append(" of ").append(data.length).append(" bytes\n");
        }
        return sb.toString();
    }
}
