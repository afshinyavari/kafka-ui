package se.afshin.yavari.kafka.ui.topics;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.DevKafkaProfile;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.KafkaPropertiesCustomizer;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.TestCertsResource;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;

@QuarkusTest
@TestProfile(DevKafkaProfile.class)
@WithTestResource(TestCertsResource.class)
class TopicsResourceTest {

    @Inject KafkaClients clients;
    @Inject KafkaUiConfig config;
    @Inject TlsMaterialHolder tls;
    @Inject KafkaPropertiesCustomizer customizer;

    static boolean created;

    private void ensureTopics() throws Exception {
        if (created) {
            return;
        }
        var creds = new Credentials.Mtls();
        try {
            clients.admin(creds).createTopics(List.of(
                    new NewTopic("orders", 3, (short) 1).configs(java.util.Map.of("retention.ms", "123456")),
                    new NewTopic("payments", 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
        try (var producer = clients.newProducer(creds)) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("orders", i % 3, null, ("v" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }
        created = true;
    }

    @Test void listsTopicsWithPartitionsAndCount() throws Exception {
        ensureTopics();
        given().get("/topics").then().statusCode(200)
                .body(matchesPattern("(?s).*<td><a href=\"/topics/orders\">orders</a></td>\\s*<td>3</td>\\s*<td>1</td>\\s*<td>5</td>.*"))
                .body(containsString("/topics/orders"));
    }

    @Test void filterByName() throws Exception {
        ensureTopics();
        given().header("HX-Request", "true").queryParam("q", "ord").get("/topics").then().statusCode(200)
                .body(containsString("orders"))
                .body(not(containsString("payments")))
                .body(not(containsString("<html")));
    }

    @Test void internalTopicsHiddenByDefault() throws Exception {
        ensureTopics();
        given().get("/topics").then().body(not(containsString("__consumer_offsets")));
    }

    @Test void detailHasTabs() throws Exception {
        ensureTopics();
        given().get("/topics/orders").then().statusCode(200)
                .body(containsString("Overview")).body(containsString("Messages"))
                .body(containsString("Consumers")).body(containsString("Settings"))
                .body(containsString("hx-get=\"/topics/orders/settings\""))
                .body(containsString("href=\"/topics\" aria-current=\"page\""))
                .body(not(containsString("href=\"/consumers\" aria-current")))
                .body(containsString("<html"));
    }

    @Test void overviewShowsPartitions() throws Exception {
        ensureTopics();
        // partition, leader, replicas, in sync, first offset, last offset, messages
        given().header("HX-Request", "true").get("/topics/orders/overview").then().statusCode(200)
                .body(not(containsString("<html")))
                .body(matchesPattern("(?s).*<td>0</td>\\s*<td>\\d+</td>\\s*<td>1</td>\\s*<td>1</td>\\s*<td>0</td>\\s*<td>2</td>\\s*<td>2</td>.*"))
                .body(matchesPattern("(?s).*<td>2</td>\\s*<td>\\d+</td>\\s*<td>1</td>\\s*<td>1</td>\\s*<td>0</td>\\s*<td>1</td>\\s*<td>1</td>.*"));
    }

    @Test void settingsMarkOverriddenAndInheritedPerRow() throws Exception {
        ensureTopics();
        given().header("HX-Request", "true").get("/topics/orders/settings").then().statusCode(200)
                .body(matchesPattern("(?s).*<td>retention\\.ms</td>\\s*<td>123456</td>\\s*<td><span class=\"badge badge-accent\">overridden</span></td>.*"))
                .body(matchesPattern("(?s).*<td>cleanup\\.policy</td>\\s*<td>delete</td>\\s*<td><span class=\"badge\">default</span></td>.*"));
    }

    @Test void htmxTabClickReturnsTabBarWithNewActiveTab() throws Exception {
        ensureTopics();
        given().header("HX-Request", "true").get("/topics/orders/settings").then()
                .header("Vary", containsString("HX-Request"))
                .body(matchesPattern("(?s).*class=\"active\">Settings</a>.*"))
                .body(not(containsString("class=\"active\">Overview")));
    }

    @Test void historyRestoreGetsFullPage() throws Exception {
        ensureTopics();
        given().header("HX-Request", "true").header("HX-History-Restore-Request", "true")
                .get("/topics/orders/settings").then().body(containsString("<html"));
    }

    @Test void settingsAreReadOnly() throws Exception {
        ensureTopics();
        given().get("/topics/orders/settings").then().statusCode(200)
                .body(containsString("retention.ms"))
                .body(not(containsString("<form")))
                .body(not(containsString("Edit")))
                .body(not(containsString("Delete")));
    }

    @Test void unknownTopicShowsFriendlyNotFound() {
        given().get("/topics/nope").then().statusCode(404)
                .body(containsString("Topic nope does not exist."));
    }

    @Test void noCreateOrDeleteActions() throws Exception {
        ensureTopics();
        given().get("/topics").then().statusCode(200)
                .body(not(containsString("Add a Topic")))
                .body(not(containsString("Delete")));
    }

    /** Like a consumer-only grant (Describe + Read): every admin call works except DescribeConfigs. */
    private void denyDescribeConfigs() {
        Admin real = clients.admin(new Credentials.Mtls());
        Admin denying = (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("describeConfigs")) {
                        @SuppressWarnings("unchecked")
                        var resources = (java.util.Collection<ConfigResource>) args[0];
                        java.util.Map<ConfigResource, KafkaFuture<Config>> futures = new java.util.HashMap<>();
                        for (ConfigResource r : resources) {
                            var f = new KafkaFutureImpl<Config>();
                            f.completeExceptionally(new TopicAuthorizationException("denied"));
                            futures.put(r, f);
                        }
                        return new DescribeConfigsResult(futures) { };
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        QuarkusMock.installMockForType(new KafkaClients(config, tls, customizer) {
            @Override
            public Admin admin(Credentials credentials) {
                return denying;
            }
        }, KafkaClients.class);
    }

    @Test void deniedSettingsOnlyAffectTheSettingsTab() throws Exception {
        ensureTopics();
        denyDescribeConfigs();
        given().header("HX-Request", "true").get("/topics/orders/settings").then().statusCode(200)
                .body(containsString("You don&#39;t have permission to view this topic&#39;s settings."))
                .body(not(containsString("retention.ms")));
        given().header("HX-Request", "true").get("/topics/orders/overview").then().statusCode(200)
                .body(containsString("<td>0</td>"));
        given().header("HX-Request", "true").get("/topics/orders/messages").then().statusCode(200)
                .body(containsString("search-form"));
        given().get("/topics/orders/consumers").then().statusCode(200);
    }
}
