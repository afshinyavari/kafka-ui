package se.afshin.yavari.kafka.ui.messages;

import jakarta.inject.Singleton;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.util.List;
import se.afshin.yavari.kafka.ui.registry.IdKind;
import se.afshin.yavari.kafka.ui.registry.Lookup;
import se.afshin.yavari.kafka.ui.registry.RegistryClient;
import se.afshin.yavari.kafka.ui.registry.RegistrySchema;
import se.afshin.yavari.kafka.ui.registry.SchemaCache;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** Test seam: contentId 7 is the Avro schema default/order-value v3, everything else is not found. */
@Alternative
@Singleton
public class StubSchemaCache extends SchemaCache {

    static final String ORDER =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

    @Inject
    public StubSchemaCache(RegistryClient client) {
        super(client);
    }

    @Override
    public Lookup get(Credentials credentials, IdKind kind, long id) {
        return kind == IdKind.CONTENT_ID && id == 7
                ? new Lookup.Found(new RegistrySchema("AVRO", ORDER, "default", "order-value", "3", List.of()))
                : new Lookup.NotFound();
    }
}
