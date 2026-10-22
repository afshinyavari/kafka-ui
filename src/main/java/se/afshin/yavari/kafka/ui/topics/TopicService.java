package se.afshin.yavari.kafka.ui.topics;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.Node;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.Permissions;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** Read-only topic queries. */
@ApplicationScoped
public class TopicService {

    static final String MASKED = "******";
    public static final String SETTINGS_DENIED = "You don't have permission to view this topic's settings.";

    @Inject KafkaClients clients;

    public List<TopicRow> list(Credentials credentials, String nameFilter, boolean showInternal) {
        String topicName = "the topic list";
        try {
            Admin admin = clients.admin(credentials);
            String filter = nameFilter == null ? "" : nameFilter.trim().toLowerCase(Locale.ROOT);
            var names = admin.listTopics(new ListTopicsOptions().listInternal(showInternal)).names().get().stream()
                    .filter(n -> filter.isEmpty() || n.toLowerCase(Locale.ROOT).contains(filter))
                    .sorted().toList();
            if (names.isEmpty()) {
                return List.of();
            }
            Map<String, TopicDescription> descriptions = admin.describeTopics(names).allTopicNames().get();
            Map<TopicPartition, Long> starts = offsets(admin, descriptions.values(), OffsetSpec.earliest());
            Map<TopicPartition, Long> ends = offsets(admin, descriptions.values(), OffsetSpec.latest());
            List<TopicRow> rows = new ArrayList<>();
            for (String name : names) {
                TopicDescription d = descriptions.get(name);
                Long count = 0L;
                for (var p : d.partitions()) {
                    var tp = new TopicPartition(name, p.partition());
                    Long s = starts.get(tp);
                    Long en = ends.get(tp);
                    if (s == null || en == null) {
                        count = null;
                        break;
                    }
                    count += en - s;
                }
                int rf = d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
                rows.add(new TopicRow(name, d.partitions().size(), rf, count, d.isInternal()));
            }
            rows.sort(Comparator.comparing(TopicRow::name));
            return rows;
        } catch (Exception e) {
            throw wrap(e, topicName);
        }
    }

    /** Partitions and permissions only; needs Describe on the topic (a typical consumer grant). */
    public TopicInfo describe(Credentials credentials, String topic) {
        return describe(credentials, topic, false);
    }

    /**
     * Also loads the topic configuration (DescribeConfigs). When Kafka denies that, the info carries
     * {@link #SETTINGS_DENIED} instead of failing.
     */
    public TopicInfo describeWithSettings(Credentials credentials, String topic) {
        return describe(credentials, topic, true);
    }

    private TopicInfo describe(Credentials credentials, String topic, boolean withSettings) {
        String what = "topic " + topic;
        try {
            Admin admin = clients.admin(credentials);
            TopicDescription d = admin.describeTopics(List.of(topic),
                    new DescribeTopicsOptions().includeAuthorizedOperations(true)).allTopicNames().get().get(topic);
            Map<TopicPartition, Long> starts = offsets(admin, List.of(d), OffsetSpec.earliest());
            Map<TopicPartition, Long> ends = offsets(admin, List.of(d), OffsetSpec.latest());
            List<PartitionRow> partitions = d.partitions().stream()
                    .sorted(Comparator.comparingInt(p -> p.partition()))
                    .map(p -> {
                        var tp = new TopicPartition(topic, p.partition());
                        return new PartitionRow(p.partition(), p.leader() == null ? -1 : p.leader().id(),
                                p.replicas().stream().map(Node::id).toList(),
                                p.isr().stream().map(Node::id).toList(), starts.get(tp), ends.get(tp));
                    }).toList();
            Map<String, ConfigRow> configs = new TreeMap<>();
            String denied = null;
            if (withSettings) {
                try {
                    configs = configs(admin, topic);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof AuthorizationException)) {
                        throw e;
                    }
                    denied = SETTINGS_DENIED;
                }
            }
            return new TopicInfo(topic, partitions, configs, Permissions.canProduce(d), denied);
        } catch (Exception e) {
            throw wrap(e, what);
        }
    }

    private static Map<String, ConfigRow> configs(Admin admin, String topic)
            throws ExecutionException, InterruptedException {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Map<String, ConfigRow> configs = new TreeMap<>();
        for (ConfigEntry e : admin.describeConfigs(List.of(resource)).all().get().get(resource).entries()) {
            String value = e.isSensitive() ? MASKED : e.value();
            configs.put(e.name(), new ConfigRow(e.name(), value,
                    e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG));
        }
        return configs;
    }

    private static Map<TopicPartition, Long> offsets(Admin admin, Iterable<TopicDescription> topics, OffsetSpec spec)
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicDescription d : topics) {
            d.partitions().forEach(p -> request.put(new TopicPartition(d.name(), p.partition()), spec));
        }
        Map<TopicPartition, Long> result = new HashMap<>();
        for (var entry : admin.listOffsets(request).all().get().entrySet()) {
            ListOffsetsResultInfo info = entry.getValue();
            result.put(entry.getKey(), info.offset());
        }
        return result;
    }

    private static RuntimeException wrap(Exception e, String what) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new UserErrorException(KafkaErrors.translate(e, what));
    }
}
