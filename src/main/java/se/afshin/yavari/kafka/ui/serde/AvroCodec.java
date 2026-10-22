package se.afshin.yavari.kafka.ui.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/** Avro binary/JSON-encoded bodies to plain JSON (no union wrapping) and back. */
public final class AvroCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonAvroConverter CONVERTER = new JsonAvroConverter();
    private static final Pattern FIELD = Pattern.compile("[Ff]ield[: ]+\"?([\\w.\\[\\]]+)\"?");

    private AvroCodec() {}

    /**
     * Parses the schema; referenced schemas are parsed first into the same parser so their names resolve.
     * Nested references of references are not followed.
     */
    public static Schema parse(RegistrySchema schema, Function<RegistrySchema.Reference, String> referenceContent) {
        Schema.Parser parser = new Schema.Parser();
        for (RegistrySchema.Reference ref : schema.references() == null ? List.<RegistrySchema.Reference>of() : schema.references()) {
            parser.parse(referenceContent.apply(ref));
        }
        return parser.parse(schema.content());
    }

    /** Decodes {@code body} (Avro binary, or Avro JSON encoding when {@code jsonEncoded}) to plain JSON. */
    public static String toJson(Schema writer, byte[] body, boolean jsonEncoded) {
        try {
            Decoder decoder = jsonEncoded
                    ? DecoderFactory.get().jsonDecoder(writer, new String(body, StandardCharsets.UTF_8))
                    : DecoderFactory.get().binaryDecoder(body, null);
            Object datum = new GenericDatumReader<>(writer).read(null, decoder);
            if (decoder instanceof BinaryDecoder bd && !bd.isEnd()) {
                throw new IllegalStateException("unexpected trailing bytes after the Avro value");
            }
            Schema wrapper = writer;
            Object value = datum;
            boolean wrapped = writer.getType() != Schema.Type.RECORD;
            if (wrapped) {
                wrapper = Schema.createRecord("Wrapper", null, "ui", false);
                wrapper.setFields(List.of(new Schema.Field("v", writer)));
                org.apache.avro.generic.GenericData.Record r = new org.apache.avro.generic.GenericData.Record(wrapper);
                r.put("v", datum);
                value = r;
            }
            String json = new String(CONVERTER.convertToJson((GenericRecord) value), StandardCharsets.UTF_8);
            if (wrapped) {
                json = MAPPER.writeValueAsString(MAPPER.readTree(json).get("v"));
            }
            return json;
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(), e);
        }
    }

    /** Encodes plain JSON as Avro binary. */
    public static byte[] fromJson(Schema schema, String json) {
        try {
            if (schema.getType() != Schema.Type.RECORD) {
                JsonNode node = MAPPER.readTree(json);
                Schema wrapper = Schema.createRecord("Wrapper", null, "ui", false);
                wrapper.setFields(List.of(new Schema.Field("v", schema)));
                String wrapped = MAPPER.writeValueAsString(MAPPER.createObjectNode().set("v", node));
                return CONVERTER.convertToAvro(wrapped.getBytes(StandardCharsets.UTF_8), wrapper);
            }
            return CONVERTER.convertToAvro(json.getBytes(StandardCharsets.UTF_8), schema);
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            for (Throwable c = e.getCause(); c != null && c != c.getCause(); c = c.getCause()) {
                if (c.getMessage() != null) {
                    message = c.getMessage();
                    break;
                }
            }
            Matcher m = FIELD.matcher(message);
            throw new AvroJsonException(m.find() ? m.group(1) : "", message);
        }
    }
}
