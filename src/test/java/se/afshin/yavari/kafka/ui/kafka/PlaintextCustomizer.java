package se.afshin.yavari.kafka.ui.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Properties;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Test seam: rewrites client properties to talk PLAINTEXT to the Dev Services broker. Not active by default: only
 * test profiles that enable it (see {@link DevKafkaProfile}).
 */
@Alternative
@ApplicationScoped
public class PlaintextCustomizer implements KafkaPropertiesCustomizer {

    @Override
    public Properties customize(Properties in) {
        Properties p = new Properties();
        in.forEach((k, v) -> {
            String key = k.toString();
            if (!(key.startsWith("ssl.") || key.startsWith("sasl."))) {
                p.put(k, v);
            }
        });
        p.put("security.protocol", "PLAINTEXT");
        p.put("bootstrap.servers", ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class));
        return p;
    }
}
