package se.afshin.yavari.kafka.ui.serde;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import se.afshin.yavari.kafka.ui.registry.IdKind;

/** Apicurio wire format: schema id in record headers or as a magic-byte payload prefix. */
public final class WireFormat {

    public static final String DEFAULT_KEY_PREFIX = "apicurio.key.";
    public static final String DEFAULT_VALUE_PREFIX = "apicurio.value.";

    private static final String GLOBAL_ID = "globalId";
    private static final String CONTENT_ID = "contentId";
    private static final String ENCODING = "encoding";

    /** A possible schema reference found in a payload, with the offset where the body starts. */
    public record Candidate(SchemaRef ref, int bodyOffset) {}

    private WireFormat() {}

    public static Optional<SchemaRef> fromHeaders(Headers h, boolean isKey) {
        return fromHeaders(h, DEFAULT_KEY_PREFIX, DEFAULT_VALUE_PREFIX, isKey);
    }

    public static Optional<SchemaRef> fromHeaders(Headers h, String keyPrefix, String valuePrefix, boolean isKey) {
        String prefix = isKey ? keyPrefix : valuePrefix;
        Optional<SchemaRef> global = readId(h, prefix + GLOBAL_ID, IdKind.GLOBAL_ID);
        return global.isPresent() ? global : readId(h, prefix + CONTENT_ID, IdKind.CONTENT_ID);
    }

    public static Optional<String> encodingHeader(Headers h, boolean isKey) {
        return encodingHeader(h, DEFAULT_KEY_PREFIX, DEFAULT_VALUE_PREFIX, isKey);
    }

    public static Optional<String> encodingHeader(Headers h, String keyPrefix, String valuePrefix, boolean isKey) {
        Header header = h.lastHeader((isKey ? keyPrefix : valuePrefix) + ENCODING);
        if (header == null || header.value() == null) {
            return Optional.empty();
        }
        return Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }

    public static List<Candidate> payloadCandidates(byte[] data) {
        List<Candidate> out = new ArrayList<>(2);
        if (data == null || data.length < 5 || data[0] != 0) {
            return out;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        out.add(new Candidate(
                new SchemaRef(IdKind.CONTENT_ID, buf.getInt(1), SchemaRef.Source.PAYLOAD_4), 5));
        if (data.length >= 9) {
            out.add(new Candidate(
                    new SchemaRef(IdKind.GLOBAL_ID, buf.getLong(1), SchemaRef.Source.PAYLOAD_8), 9));
        }
        return out;
    }

    public static byte[] frame(IdKind kind, long id, byte[] body) {
        if (kind == IdKind.CONTENT_ID) {
            if (id < 0 || id > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("contentId does not fit in 4 bytes: " + id);
            }
            return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt((int) id).put(body).array();
        }
        return ByteBuffer.allocate(9 + body.length).put((byte) 0).putLong(id).put(body).array();
    }

    public static void writeHeader(
            Headers h, boolean isKey, IdKind kind, long id, String keyPrefix, String valuePrefix) {
        String name = (isKey ? keyPrefix : valuePrefix) + (kind == IdKind.GLOBAL_ID ? GLOBAL_ID : CONTENT_ID);
        h.remove(name);
        h.add(name, ByteBuffer.allocate(8).putLong(id).array());
    }

    private static Optional<SchemaRef> readId(Headers h, String name, IdKind kind) {
        Header header = h.lastHeader(name);
        if (header == null || header.value() == null) {
            return Optional.empty();
        }
        byte[] v = header.value();
        long id;
        if (v.length == 8) {
            id = ByteBuffer.wrap(v).getLong();
        } else if (v.length == 4) {
            id = ByteBuffer.wrap(v).getInt();
        } else {
            return Optional.empty();
        }
        return Optional.of(new SchemaRef(kind, id, SchemaRef.Source.HEADER));
    }
}
