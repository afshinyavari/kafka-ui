package se.afshin.yavari.kafka.ui.messages;

import java.util.Set;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.kafka.PlaintextCustomizer;

/** Dev Services Kafka plus a canned schema registry. */
public class MessagesProfile extends DevKafkaProfile {

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
        return Set.of(PlaintextCustomizer.class, StubSchemaCache.class);
    }
}
