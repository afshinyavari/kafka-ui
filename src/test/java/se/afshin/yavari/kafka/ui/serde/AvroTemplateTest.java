package se.afshin.yavari.kafka.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class AvroTemplateTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String CUSTOMER = """
            {"type":"record","name":"Customer","namespace":"demo","fields":[
              {"name":"id","type":"string"},
              {"name":"age","type":"int"},
              {"name":"score","type":"double"},
              {"name":"active","type":"boolean"},
              {"name":"country","type":"string","default":"SE"},
              {"name":"tier","type":{"type":"enum","name":"Tier","symbols":["GOLD","SILVER"]}},
              {"name":"email","type":["null","string"],"default":null},
              {"name":"address","type":["null",{"type":"record","name":"Address","fields":[
                {"name":"street","type":"string"},{"name":"zip","type":"int"}]}],"default":null},
              {"name":"tags","type":{"type":"array","items":"string"}},
              {"name":"attributes","type":{"type":"map","values":"long"}},
              {"name":"createdAt","type":{"type":"long","logicalType":"timestamp-millis"}}
            ]}""";

    @Test
    void everyFieldIsPresentWithAnEmptyValueOrItsDefault() throws Exception {
        JsonNode t = JSON.readTree(AvroTemplate.of(new Schema.Parser().parse(CUSTOMER)));
        assertThat(t.get("id").asText()).isEmpty();
        assertThat(t.get("age").isInt()).isTrue();
        assertThat(t.get("score").isDouble()).isTrue();
        assertThat(t.get("active").asBoolean()).isFalse();
        assertThat(t.get("country").asText()).isEqualTo("SE");
        assertThat(t.get("tier").asText()).isEqualTo("GOLD");
        assertThat(t.get("email").isTextual()).as("optional field shows its non-null type").isTrue();
        assertThat(t.get("address").get("street").isTextual()).isTrue();
        assertThat(t.get("address").get("zip").isInt()).isTrue();
        assertThat(t.get("tags").get(0).isTextual()).isTrue();
        assertThat(t.get("attributes").get("key").isNumber()).isTrue();
        assertThat(t.get("createdAt").isNumber()).isTrue();
    }

    @Test
    void theTemplateIsAcceptedByTheProducer() {
        Schema schema = new Schema.Parser().parse(CUSTOMER);
        assertThat(AvroCodec.fromJson(schema, AvroTemplate.of(schema))).isNotEmpty();
    }

    @Test
    void recursiveRecordsEndInNull() throws Exception {
        Schema node = new Schema.Parser().parse("""
                {"type":"record","name":"Node","fields":[
                  {"name":"name","type":"string"},
                  {"name":"next","type":["null","Node"],"default":null}]}""");
        JsonNode t = JSON.readTree(AvroTemplate.of(node));
        assertThat(t.get("name").isTextual()).isTrue();
        assertThat(t.get("next").isNull()).as("a record inside itself is not expanded").isTrue();
        assertThat(AvroCodec.fromJson(node, AvroTemplate.of(node))).isNotEmpty();
    }

    @Test
    void nonRecordSchemas() {
        assertThat(AvroTemplate.of(Schema.create(Schema.Type.STRING))).isEqualTo("\"\"");
        assertThat(AvroTemplate.of(new Schema.Parser().parse("{\"type\":\"array\",\"items\":\"int\"}")))
                .isEqualTo("[ 0 ]");
    }
}
