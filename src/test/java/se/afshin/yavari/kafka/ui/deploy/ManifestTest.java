package se.afshin.yavari.kafka.ui.deploy;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

class ManifestTest {
    private static final Path DIR = Path.of("deploy/kubernetes");

    @SuppressWarnings("unchecked")
    private static Map<String, Object> load(String file) throws IOException {
        return new Yaml().load(Files.readString(DIR.resolve(file)));
    }

    @SuppressWarnings("unchecked")
    private static <T> T at(Object node, String... path) {
        for (String p : path) {
            node = ((Map<String, Object>) node).get(p);
        }
        return (T) node;
    }

    @Test
    @SuppressWarnings("unchecked")
    void manifestsParseAndReferenceConfigurableKeys() throws IOException {
        for (String f : List.of("service.yaml", "configmap.yaml", "kustomization.yaml")) {
            assertThat(load(f)).as(f).isNotEmpty();
        }
        var deployment = load("deployment.yaml");
        List<Map<String, Object>> volumes = at(deployment, "spec", "template", "spec", "volumes");
        var volumesByName = volumes.stream().collect(Collectors.toMap(v -> (String) v.get("name"), v -> v));
        assertThat(volumesByName).containsKeys("client-cert", "ca-bundle");

        List<Map<String, String>> certItems = at(volumesByName.get("client-cert"), "secret", "items");
        assertThat(certItems).extracting(i -> i.get("key")).containsExactly("tls.crt", "tls.key");
        List<Map<String, String>> caItems = at(volumesByName.get("ca-bundle"), "secret", "items");
        assertThat(caItems).extracting(i -> i.get("key")).containsExactly("ca-bundle.pem");

        List<Map<String, Object>> containers = at(deployment, "spec", "template", "spec", "containers");
        var container = containers.get(0);
        List<Map<String, String>> mounts = (List<Map<String, String>>) container.get("volumeMounts");
        assertThat(mounts).extracting(m -> m.get("mountPath"))
                .containsExactlyInAnyOrder("/etc/kafka-ui/client", "/etc/kafka-ui/ca");

        var configMap = load("configmap.yaml");
        Map<String, String> data = at(configMap, "data");
        assertThat(data)
                .containsEntry("KAFKA_UI_MODE", "oidc")
                .containsEntry("KAFKA_UI_TLS_CA_FILE", "/etc/kafka-ui/ca/ca.pem")
                // the client certificate is mTLS-only: present but commented out for the default oidc mode
                .doesNotContainKeys("KAFKA_UI_TLS_CERT_FILE", "KAFKA_UI_TLS_KEY_FILE");
        String raw = Files.readString(DIR.resolve("configmap.yaml"));
        assertThat(raw)
                .contains("# KAFKA_UI_TLS_CERT_FILE: /etc/kafka-ui/client/tls.crt")
                .contains("# KAFKA_UI_TLS_KEY_FILE: /etc/kafka-ui/client/tls.key");
        assertThat(container.get("envFrom").toString()).contains("kafka-ui");
    }
}
