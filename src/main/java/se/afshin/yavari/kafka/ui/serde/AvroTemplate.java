package se.afshin.yavari.kafka.ui.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

/**
 * A plain-JSON skeleton of an Avro schema for the produce form, in the format {@link AvroCodec#fromJson} reads:
 * every field present, with its default when the schema has a simple one, else an empty value of its type. Optional
 * fields (unions with null) show the first non-null branch so their structure is visible.
 */
public final class AvroTemplate {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private AvroTemplate() {}

    public static String of(Schema schema) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(sample(schema, new HashSet<>()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@code path} holds the records being expanded, so a recursive record ends in null instead of looping. */
    private static JsonNode sample(Schema s, Set<String> path) {
        return switch (s.getType()) {
            case RECORD -> {
                if (!path.add(s.getFullName())) {
                    yield NODES.nullNode();
                }
                ObjectNode o = NODES.objectNode();
                for (Schema.Field f : s.getFields()) {
                    JsonNode def = simpleDefault(f);
                    o.set(f.name(), def != null ? def : sample(f.schema(), path));
                }
                path.remove(s.getFullName());
                yield o;
            }
            case UNION -> {
                Schema branch = s.getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst()
                        .orElse(s.getTypes().get(0));
                yield sample(branch, path);
            }
            case ENUM -> NODES.textNode(s.getEnumDefault() != null ? s.getEnumDefault() : s.getEnumSymbols().get(0));
            case ARRAY -> {
                ArrayNode a = NODES.arrayNode();
                a.add(sample(s.getElementType(), path));
                yield a;
            }
            case MAP -> {
                ObjectNode m = NODES.objectNode();
                m.set("key", sample(s.getValueType(), path));
                yield m;
            }
            case STRING, BYTES, FIXED -> NODES.textNode("");
            case INT, LONG -> NODES.numberNode(0);
            case FLOAT, DOUBLE -> NODES.numberNode(0.0);
            case BOOLEAN -> NODES.booleanNode(false);
            case NULL -> NODES.nullNode();
        };
    }

    /** The field's default when it is a scalar; null (use the type's sample) otherwise or when there is none. */
    private static JsonNode simpleDefault(Schema.Field f) {
        if (!f.hasDefaultValue()) {
            return null;
        }
        Object v = f.defaultVal();
        if (v == JsonProperties.NULL_VALUE) {
            // `"default": null` marks an optional field; still show its structure
            return null;
        }
        if (v instanceof String || v instanceof Number || v instanceof Boolean) {
            return MAPPER.valueToTree(v);
        }
        return null;
    }
}
