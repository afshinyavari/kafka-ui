package se.afshin.yavari.kafka.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;

class AvroCodecTest {

    static final String ORDER = """
            {"type":"record","name":"Order","namespace":"shop","fields":[
              {"name":"id","type":"string"},
              {"name":"amount","type":"double"},
              {"name":"note","type":["null","string"],"default":null}]}""";

    @Test
    void fromJsonReportsFieldPath() {
        Schema schema = new Schema.Parser().parse(ORDER);
        assertThatThrownBy(() -> AvroCodec.fromJson(schema, "{\"id\":\"o-1\"}"))
                .isInstanceOfSatisfying(AvroJsonException.class, e -> assertThat(e.fieldPath()).isEqualTo("amount"));
    }

    @Test
    void roundTripPlainJsonWithUnion() throws Exception {
        Schema schema = new Schema.Parser().parse(ORDER);
        byte[] avro = AvroCodec.fromJson(schema, "{\"id\":\"o-1\",\"amount\":10.5,\"note\":\"hi\"}");
        String json = AvroCodec.toJson(schema, avro, false);
        assertThat(json).contains("\"note\":\"hi\"").doesNotContain("{\"string\"");
        assertThat(AvroCodec.toJson(schema, AvroCodec.fromJson(schema, "{\"id\":\"x\",\"amount\":1.0,\"note\":null}"), false))
                .contains("\"note\":null");
    }

    @Test
    void nonRecordSchemaWorks() throws Exception {
        Schema schema = new Schema.Parser().parse("{\"type\":\"array\",\"items\":\"int\"}");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<Object>(schema).write(new GenericData.Array<>(schema, java.util.List.of(1, 2)), enc);
        enc.flush();
        assertThat(AvroCodec.toJson(schema, out.toByteArray(), false)).isEqualTo("[1,2]");
    }

    @Test
    void parseResolvesReferences() {
        String address = "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"shop\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";
        String main = "{\"type\":\"record\",\"name\":\"Customer\",\"namespace\":\"shop\",\"fields\":[{\"name\":\"addr\",\"type\":\"shop.Address\"}]}";
        var ref = new RegistrySchema.Reference("shop.Address", "g", "address", "1");
        RegistrySchema s = new RegistrySchema("AVRO", main, "g", "customer", "1", java.util.List.of(ref));
        Schema schema = AvroCodec.parse(s, r -> address);
        assertThat(schema.getField("addr").schema().getName()).isEqualTo("Address");
    }
}
