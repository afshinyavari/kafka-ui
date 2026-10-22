package se.afshin.yavari.kafka.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.security.Credentials;

class MessageDecoderTest {

    private static final Credentials CREDS = new Credentials.Mtls();
    private static final String ORDER = AvroCodecTest.ORDER;
    private static final Schema SCHEMA = new Schema.Parser().parse(ORDER);
    private static final RegistrySchema AVRO =
            new RegistrySchema("AVRO", ORDER, "shop", "orders-value", "3", List.of());

    private final Map<String, Lookup> lookups = new HashMap<>();
    private final MessageDecoder decoder = new MessageDecoder(
            (c, kind, id) -> lookups.getOrDefault(kind + ":" + id, new Lookup.NotFound()), (c, r) -> {
                throw new IllegalStateException("no references expected");
            });

    private static byte[] avroBody(boolean json) throws Exception {
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("id", "o-1");
        r.put("amount", 10.5);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = json
                ? EncoderFactory.get().jsonEncoder(SCHEMA, out)
                : EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(SCHEMA).write(r, enc);
        enc.flush();
        return out.toByteArray();
    }

    private static RecordHeaders header(IdKind kind, long id) {
        RecordHeaders h = new RecordHeaders();
        WireFormat.writeHeader(h, false, kind, id, WireFormat.DEFAULT_KEY_PREFIX, WireFormat.DEFAULT_VALUE_PREFIX);
        return h;
    }

    private Decoded value(byte[] data, RecordHeaders h) {
        return decoder.decode(CREDS, data, h, false);
    }

    private Decoded value(String s) {
        return value(s.getBytes(StandardCharsets.UTF_8), new RecordHeaders());
    }

    @Test
    void avroViaValueHeader() throws Exception {
        lookups.put("CONTENT_ID:42", new Lookup.Found(AVRO));
        Decoded d = value(avroBody(false), header(IdKind.CONTENT_ID, 42));
        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(d.text().replaceAll("\\s", "")).isEqualTo("{\"id\":\"o-1\",\"amount\":10.5,\"note\":null}");
        assertThat(d.explanation()).isEqualTo("Avro – schema shop/orders-value v3, contentId 42 from header");
        assertThat(d.ref()).isPresent();
    }

    @Test
    void avroViaFourBytePayload() throws Exception {
        lookups.put("CONTENT_ID:42", new Lookup.Found(AVRO));
        Decoded d = value(WireFormat.frame(IdKind.CONTENT_ID, 42, avroBody(false)), new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(d.explanation()).contains("from payload (4-byte contentId)");
    }

    @Test
    void avroViaEightBytePayloadWhenFourByteIdUnknown() throws Exception {
        lookups.put("GLOBAL_ID:7", new Lookup.Found(AVRO));
        Decoded d = value(WireFormat.frame(IdKind.GLOBAL_ID, 7, avroBody(false)), new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(d.explanation()).contains("8-byte globalId").contains("globalId 7");
    }

    @Test
    void avroJsonEncodingHeader() throws Exception {
        lookups.put("CONTENT_ID:42", new Lookup.Found(AVRO));
        RecordHeaders h = header(IdKind.CONTENT_ID, 42);
        h.add("apicurio.value.encoding", "JSON".getBytes(StandardCharsets.UTF_8));
        Decoded d = value(avroBody(true), h);
        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(d.text()).contains("\"id\" : \"o-1\"").contains("10.5");
    }

    @Test
    void jsonSchemaArtifact() {
        lookups.put("CONTENT_ID:5",
                new Lookup.Found(new RegistrySchema("JSON", "{}", "shop", "evt", "1", List.of())));
        Decoded d = value("{\"a\":1}".getBytes(StandardCharsets.UTF_8), header(IdKind.CONTENT_ID, 5));
        assertThat(d.format()).isEqualTo(Format.JSON_SCHEMA);
        assertThat(d.text()).isEqualTo("{\n  \"a\" : 1\n}");
        assertThat(d.explanation()).startsWith("JSON Schema – schema shop/evt v1");
    }

    @Test
    void unsupportedSchemaTypeFallsBack() {
        lookups.put("CONTENT_ID:5",
                new Lookup.Found(new RegistrySchema("PROTOBUF", "x", "g", "a", "1", List.of())));
        Decoded d = value("{\"a\":1}".getBytes(StandardCharsets.UTF_8), header(IdKind.CONTENT_ID, 5));
        assertThat(d.format()).isEqualTo(Format.JSON);
        assertThat(d.explanation()).startsWith("Unsupported schema type PROTOBUF – ");
    }

    @Test
    void zeroBytePayloadThatIsNotApicurioFallsBackToHex() {
        byte[] data = {0, 0, 0, 0, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Decoded d = value(data, new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.HEX);
        assertThat(d.explanation()).contains("No schema found for id");
        assertThat(d.text()).startsWith("00000000  00 00 00 00 07 01 02 03");
    }

    @Test
    void deniedSchemaFallsBackWithExplanation() {
        lookups.put("CONTENT_ID:5", new Lookup.Denied(403));
        Decoded d = value("{\"a\":1}".getBytes(StandardCharsets.UTF_8), header(IdKind.CONTENT_ID, 5));
        assertThat(d.format()).isEqualTo(Format.JSON);
        assertThat(d.explanation()).startsWith("No permission to read schema contentId 5 – ");
    }

    @Test
    void unavailableRegistryIsExplained() {
        lookups.put("CONTENT_ID:5", new Lookup.Unavailable("timeout"));
        Decoded d = value("hello".getBytes(StandardCharsets.UTF_8), header(IdKind.CONTENT_ID, 5));
        assertThat(d.format()).isEqualTo(Format.TEXT);
        assertThat(d.explanation()).startsWith("Schema registry unavailable – ");
    }

    @Test
    void plainJson() {
        Decoded d = value("{\"a\":[1,2]}");
        assertThat(d.format()).isEqualTo(Format.JSON);
        assertThat(d.text()).contains("\n").contains("\"a\" : [ 1, 2 ]");
        assertThat(d.explanation()).isEqualTo("No schema reference – detected JSON");
    }

    @Test
    void xml() {
        Decoded d = value("<a><b>1</b></a>");
        assertThat(d.format()).isEqualTo(Format.XML);
        assertThat(d.text()).isEqualTo("<a>\n  <b>1</b>\n</a>");
    }

    @Test
    void xmlWithExternalEntityIsNotResolved() {
        String xml = "<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><x>&e;</x>";
        Decoded d = value(xml);
        assertThat(d.format()).isEqualTo(Format.TEXT);
        assertThat(d.text()).isEqualTo(xml).doesNotContain("root:");
    }

    @Test
    void utf8Text() {
        Decoded d = value("plain åäö text");
        assertThat(d.format()).isEqualTo(Format.TEXT);
        assertThat(d.text()).isEqualTo("plain åäö text");
    }

    @Test
    void binary() {
        Decoded d = value(new byte[] {(byte) 0xff, (byte) 0xfe, 'A', 1}, new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.HEX);
        assertThat(d.text()).contains("ff fe 41 01").contains("|..A.|");
    }

    @Test
    void hexIsTruncatedAt4Kb() {
        Decoded d = value(new byte[5000], new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.HEX);
        assertThat(d.text()).contains("truncated, showing first 4096 of 5000 bytes");
    }

    @Test
    void tombstoneAndEmpty() {
        Decoded t = decoder.decode(CREDS, null, new RecordHeaders(), true);
        assertThat(t.format()).isEqualTo(Format.NULL);
        assertThat(t.explanation()).isEqualTo("Tombstone (null)");
        assertThat(value(new byte[0], new RecordHeaders()).format()).isEqualTo(Format.EMPTY);
    }

    @Test
    void corruptAvroBodyFallsBack() throws Exception {
        lookups.put("CONTENT_ID:42", new Lookup.Found(AVRO));
        byte[] body = avroBody(false);
        byte[] truncated = java.util.Arrays.copyOf(body, 3);
        Decoded d = value(truncated, header(IdKind.CONTENT_ID, 42));
        assertThat(d.format()).isIn(Format.HEX, Format.TEXT);
        assertThat(d.explanation()).contains("Avro decoding failed");
    }

    @Test
    void manualOverrideToText() throws Exception {
        lookups.put("CONTENT_ID:42", new Lookup.Found(AVRO));
        Decoded d = decoder.decodeAs(CREDS, avroBody(false), header(IdKind.CONTENT_ID, 42), false, Format.TEXT);
        assertThat(d.format()).isEqualTo(Format.TEXT);
        assertThat(d.explanation()).contains("chosen manually");
    }

    @Test
    void forcedAvroWithoutReferenceExplains() {
        Decoded d = decoder.decodeAs(CREDS, "{\"a\":1}".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), false,
                Format.AVRO);
        assertThat(d.format()).isEqualTo(Format.JSON);
        assertThat(d.explanation()).contains("Cannot show as Avro").contains("No schema reference");
        assertThat(d.ref()).isEqualTo(Optional.empty());
    }

    @Test
    void trailingBytesAfterFourByteCandidateFallThroughToEightByte() throws Exception {
        lookups.put("CONTENT_ID:0", new Lookup.Found(new RegistrySchema("AVRO", "\"int\"", "g", "ints", "1", List.of())));
        lookups.put("GLOBAL_ID:7", new Lookup.Found(AVRO));
        Decoded d = value(WireFormat.frame(IdKind.GLOBAL_ID, 7, avroBody(false)), new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.AVRO);
        assertThat(d.explanation()).contains("8-byte globalId");
        assertThat(d.text()).contains("o-1");
    }

    @Test
    void framedJsonSchemaPayload() {
        lookups.put("CONTENT_ID:5", new Lookup.Found(new RegistrySchema("JSON", "{}", "shop", "evt", "1", List.of())));
        Decoded d = value(WireFormat.frame(IdKind.CONTENT_ID, 5, "{\"a\":1}".getBytes(StandardCharsets.UTF_8)),
                new RecordHeaders());
        assertThat(d.format()).isEqualTo(Format.JSON_SCHEMA);
        assertThat(d.text()).isEqualTo("{\n  \"a\" : 1\n}");
        assertThat(d.explanation()).contains("from payload (4-byte contentId)");
    }

    @Test
    void manualTextOnInvalidUtf8SaysSo() {
        Decoded d = decoder.decodeAs(CREDS, new byte[] {(byte) 0xff, 'a'}, new RecordHeaders(), false, Format.TEXT);
        assertThat(d.explanation()).contains("invalid UTF-8 replaced");
    }

    @Test
    void hexdumpFormatIsStable() {
        byte[] data = new byte[17];
        for (int i = 0; i < 17; i++) {
            data[i] = (byte) (0x41 + i);
        }
        assertThat(MessageDecoder.hexdump(data)).isEqualTo(
                "00000000  41 42 43 44 45 46 47 48  49 4a 4b 4c 4d 4e 4f 50  |ABCDEFGHIJKLMNOP|\n"
                        + "00000010  51                                                |Q|\n");
    }

    private static final String DEEP_XML = "<a>".repeat(5000) + "</a>".repeat(5000);

    @Test
    void deeplyNestedXmlFallsBackToText() {
        Decoded auto = value(DEEP_XML);
        assertThat(auto.format()).isEqualTo(Format.TEXT);
        assertThat(auto.explanation()).isEqualTo("No schema reference – detected text");
        Decoded forced = decoder.decodeAs(CREDS, DEEP_XML.getBytes(StandardCharsets.UTF_8), new RecordHeaders(),
                false, Format.XML);
        assertThat(forced.format()).isEqualTo(Format.TEXT);
        assertThat(forced.explanation()).isEqualTo("Not valid XML – detected text");
    }

    @Test
    void stackOverflowWhileDecodingFallsBackWithExplanation() {
        var overflowing = new MessageDecoder((c, kind, id) -> {
            throw new StackOverflowError();
        }, (c, r) -> {
            throw new IllegalStateException("no references expected");
        });
        Decoded d = overflowing.decode(CREDS, "{\"a\":1}".getBytes(StandardCharsets.UTF_8),
                header(IdKind.CONTENT_ID, 42), false);
        assertThat(d.format()).isEqualTo(Format.JSON);
        assertThat(d.explanation()).startsWith("Decoding failed: StackOverflowError");
    }

    @Test
    void stackOverflowInAvroDecodingIsExplained() throws Exception {
        var withRef = new RegistrySchema("AVRO", ORDER, "shop", "orders-value", "3",
                List.of(new RegistrySchema.Reference("shop.Money", "shop", "money", "1")));
        var overflowing = new MessageDecoder((c, kind, id) -> new Lookup.Found(withRef), (c, r) -> {
            throw new StackOverflowError();
        });
        Decoded d = overflowing.decode(CREDS, avroBody(false), header(IdKind.CONTENT_ID, 42), false);
        assertThat(d.explanation()).contains("Avro decoding failed").contains("StackOverflowError");
    }
}
