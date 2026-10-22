package se.afshin.yavari.kafka.ui.overview;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** Read-only cluster and broker queries. Topic and partition counts cover non-internal topics. */
@ApplicationScoped
public class ClusterService {

    @Inject KafkaClients clients;

    public ClusterOverview overview(Credentials credentials) {
        try {
            Admin admin = clients.admin(credentials);
            var cluster = admin.describeCluster();
            String id = cluster.clusterId().get();
            Node controller = cluster.controller().get();
            int brokers = cluster.nodes().get().size();
            int topics = 0;
            int partitions = 0;
            int urp = 0;
            for (TopicDescription d : describeAll(admin).values()) {
                urp += (int) d.partitions().stream().filter(p -> p.isr().size() < p.replicas().size()).count();
                if (!d.isInternal()) {
                    topics++;
                    partitions += d.partitions().size();
                }
            }
            return new ClusterOverview(id, controller == null ? "unknown" : "Broker " + controller.id(),
                    brokers, topics, partitions, urp);
        } catch (Exception e) {
            throw wrap(e, "the cluster");
        }
    }

    public List<BrokerRow> brokers(Credentials credentials) {
        try {
            Admin admin = clients.admin(credentials);
            Collection<Node> nodes = admin.describeCluster().nodes().get();
            Map<Integer, Integer> leaders = new HashMap<>();
            for (TopicDescription d : describeAll(admin).values()) {
                d.partitions().forEach(p -> {
                    if (p.leader() != null) {
                        leaders.merge(p.leader().id(), 1, Integer::sum);
                    }
                });
            }
            return nodes.stream()
                    .map(n -> new BrokerRow(n.id(), n.host() + ":" + n.port(), n.rack(),
                            leaders.getOrDefault(n.id(), 0)))
                    .sorted(Comparator.comparingInt(BrokerRow::id)).toList();
        } catch (Exception e) {
            throw wrap(e, "the brokers");
        }
    }

    private static Map<String, TopicDescription> describeAll(Admin admin) throws Exception {
        var names = admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        if (names.isEmpty()) {
            return Map.of();
        }
        return admin.describeTopics(names).allTopicNames().get();
    }

    private static RuntimeException wrap(Exception e, String what) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new UserErrorException(KafkaErrors.translate(e, what));
    }
}
