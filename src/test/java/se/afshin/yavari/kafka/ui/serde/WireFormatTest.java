package se.afshin.yavari.kafka.ui.serde;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.registry.IdKind;

class WireFormatTest {

    private static byte[] longBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private static byte[] intBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    @Test
    void valueGlobalIdHeader() {
        Headers h = new RecordHeaders().add("apicurio.value.globalId", longBytes(42));
        assertEquals(Optional.of(new SchemaRef(IdKind.GLOBAL_ID, 42, SchemaRef.Source.HEADER)),
                WireFormat.fromHeaders(h, false));
        assertEquals(Optional.empty(), WireFormat.fromHeaders(h, true));
    }

    @Test
    void keyContentIdHeader() {
        Headers h = new RecordHeaders().add("apicurio.key.contentId", longBytes(9));
        assertEquals(Optional.of(new SchemaRef(IdKind.CONTENT_ID, 9, SchemaRef.Source.HEADER)),
                WireFormat.fromHeaders(h, true));
    }

    @Test
    void fourByteHeaderAccepted() {
        Headers h = new RecordHeaders().add("apicurio.value.contentId", intBytes(5));
        assertEquals(Optional.of(new SchemaRef(IdKind.CONTENT_ID, 5, SchemaRef.Source.HEADER)),
                WireFormat.fromHeaders(h, false));
    }

    @Test
    void oddLengthHeaderIgnored() {
        Headers h = new RecordHeaders().add("apicurio.value.globalId", new byte[3]);
        assertEquals(Optional.empty(), WireFormat.fromHeaders(h, false));
    }

    @Test
    void customPrefixes() {
        Headers h = new RecordHeaders().add("x.globalId", longBytes(3));
        assertEquals(Optional.of(new SchemaRef(IdKind.GLOBAL_ID, 3, SchemaRef.Source.HEADER)),
                WireFormat.fromHeaders(h, "x.", "y.", true));
    }

    @Test
    void encodingHeader() {
        Headers h = new RecordHeaders().add("apicurio.value.encoding", "JSON".getBytes(StandardCharsets.UTF_8));
        assertEquals(Optional.of("JSON"), WireFormat.encodingHeader(h, false));
        assertEquals(Optional.empty(), WireFormat.encodingHeader(h, true));
    }

    @Test
    void magicByteGivesFourThenEightByteCandidates() {
        byte[] data = {0, 0, 0, 0, 7, 1, 2, 3, 4, 5};
        List<WireFormat.Candidate> c = WireFormat.payloadCandidates(data);
        assertEquals(2, c.size());
        assertEquals(new SchemaRef(IdKind.CONTENT_ID, 7, SchemaRef.Source.PAYLOAD_4), c.get(0).ref());
        assertEquals(5, c.get(0).bodyOffset());
        assertEquals(IdKind.GLOBAL_ID, c.get(1).ref().kind());
        assertEquals(SchemaRef.Source.PAYLOAD_8, c.get(1).ref().source());
        assertEquals(9, c.get(1).bodyOffset());
    }

    @Test
    void shortPayloadGivesNoEightByteCandidate() {
        List<WireFormat.Candidate> c = WireFormat.payloadCandidates(new byte[] {0, 0, 0, 0, 7, 1});
        assertEquals(1, c.size());
        assertEquals(SchemaRef.Source.PAYLOAD_4, c.get(0).ref().source());
    }

    @Test
    void noMagicByteNoCandidates() {
        assertTrue(WireFormat.payloadCandidates(new byte[] {1, 0, 0, 0, 7, 1, 2, 3, 4}).isEmpty());
        assertTrue(WireFormat.payloadCandidates(new byte[] {0, 0, 0}).isEmpty());
    }

    @Test
    void frameRoundTrips() {
        byte[] body = {9, 8, 7};
        byte[] framed = WireFormat.frame(IdKind.CONTENT_ID, 7, body);
        WireFormat.Candidate first = WireFormat.payloadCandidates(framed).get(0);
        assertEquals(7, first.ref().id());
        assertArrayEquals(body, Arrays.copyOfRange(framed, first.bodyOffset(), framed.length));
    }

    @Test
    void frameGlobalIdUsesEightBytes() {
        byte[] framed = WireFormat.frame(IdKind.GLOBAL_ID, 1L << 33, new byte[] {1});
        assertEquals(10, framed.length);
        assertEquals(1L << 33, ByteBuffer.wrap(framed, 1, 8).getLong());
    }

    @Test
    void frameRejectsContentIdOverInt() {
        assertThrows(IllegalArgumentException.class,
                () -> WireFormat.frame(IdKind.CONTENT_ID, (long) Integer.MAX_VALUE + 1, new byte[0]));
    }

    @Test
    void writeHeaderRoundTrips() {
        Headers h = new RecordHeaders();
        WireFormat.writeHeader(h, false, IdKind.GLOBAL_ID, 11, "apicurio.key.", "apicurio.value.");
        assertEquals(Optional.of(new SchemaRef(IdKind.GLOBAL_ID, 11, SchemaRef.Source.HEADER)),
                WireFormat.fromHeaders(h, false));
        Headers k = new RecordHeaders();
        WireFormat.writeHeader(k, true, IdKind.CONTENT_ID, 12, "k.", "v.");
        assertEquals(8, k.lastHeader("k.contentId").value().length);
    }
}
