package se.afshin.yavari.kafka.ui.groups;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.Permissions;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** Read-only consumer group queries. Lag is end offset minus committed offset, for committed partitions only. */
@ApplicationScoped
public class GroupService {

    @Inject KafkaClients clients;

    public List<GroupRow> list(Credentials credentials, String filter) {
        String what = "the consumer group list";
        try {
            Admin admin = clients.admin(credentials);
            String f = filter == null ? "" : filter.trim().toLowerCase(Locale.ROOT);
            List<String> ids = admin.listGroups(ListGroupsOptions.forConsumerGroups()).all().get().stream()
                    .map(l -> l.groupId())
                    .filter(id -> f.isEmpty() || id.toLowerCase(Locale.ROOT).contains(f))
                    .sorted().toList();
            return rows(admin, ids, null);
        } catch (Exception e) {
            throw wrap(e, what);
        }
    }

    /** Groups with a committed offset on the topic; lag counts that topic only. */
    public List<GroupRow> forTopic(Credentials credentials, String topic) {
        String what = "the consumer groups of topic " + topic;
        try {
            Admin admin = clients.admin(credentials);
            List<String> ids = admin.listGroups(ListGroupsOptions.forConsumerGroups()).all().get().stream()
                    .map(l -> l.groupId()).sorted().toList();
            return rows(admin, ids, topic);
        } catch (Exception e) {
            throw wrap(e, what);
        }
    }

    public GroupDetail describe(Credentials credentials, String groupId) {
        String what = "consumer group " + groupId;
        try {
            Admin admin = clients.admin(credentials);
            ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(groupId),
                    new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
                    .all().get().get(groupId);
            Map<TopicPartition, OffsetAndMetadata> committed = committed(admin, List.of(groupId))
                    .getOrDefault(groupId, Map.of());
            if (d.groupState() == GroupState.DEAD && committed.isEmpty()) {
                throw new UserErrorException(new UserError("Not found",
                        "Consumer group " + groupId + " does not exist.", 404));
            }
            Map<TopicPartition, Long> ends = ends(admin, committed.keySet());
            List<OffsetRow> offsets = committed.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(TP_ORDER))
                    .map(e -> {
                        long end = ends.get(e.getKey());
                        long c = e.getValue().offset();
                        return new OffsetRow(e.getKey().topic(), e.getKey().partition(), c, end, Math.max(0, end - c));
                    }).toList();
            List<MemberRow> members = d.members().stream()
                    .map(m -> new MemberRow(m.consumerId(), m.clientId(), m.host(),
                            m.assignment().topicPartitions().stream().sorted(TP_ORDER)
                                    .map(tp -> tp.topic() + "-" + tp.partition())
                                    .collect(java.util.stream.Collectors.joining(", "))))
                    .sorted(Comparator.comparing(MemberRow::memberId)).toList();
            return new GroupDetail(groupId, d.groupState().name(), members, offsets, Permissions.canResetOffsets(d));
        } catch (UserErrorException e) {
            throw e;
        } catch (Exception e) {
            throw wrap(e, what);
        }
    }

    private List<GroupRow> rows(Admin admin, List<String> ids, String topic)
            throws ExecutionException, InterruptedException {
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<String, ConsumerGroupDescription> descriptions = admin.describeConsumerGroups(ids).all().get();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> committed = committed(admin, ids);
        Set<TopicPartition> all = new HashSet<>();
        committed.values().forEach(m -> m.keySet().stream()
                .filter(tp -> topic == null || tp.topic().equals(topic)).forEach(all::add));
        Map<TopicPartition, Long> ends = ends(admin, all);
        List<GroupRow> rows = new ArrayList<>();
        for (String id : ids) {
            var offsets = committed.getOrDefault(id, Map.of()).entrySet().stream()
                    .filter(e -> topic == null || e.getKey().topic().equals(topic)).toList();
            if (topic != null && offsets.isEmpty()) {
                continue;
            }
            long lag = 0;
            for (var e : offsets) {
                lag += Math.max(0, ends.get(e.getKey()) - e.getValue().offset());
            }
            int topics = (int) offsets.stream().map(e -> e.getKey().topic()).distinct().count();
            ConsumerGroupDescription d = descriptions.get(id);
            rows.add(new GroupRow(id, d.groupState().name(), d.members().size(), lag, topics));
        }
        return rows;
    }

    private static Map<String, Map<TopicPartition, OffsetAndMetadata>> committed(Admin admin, List<String> ids)
            throws ExecutionException, InterruptedException {
        Map<String, ListConsumerGroupOffsetsSpec> specs = new HashMap<>();
        ids.forEach(id -> specs.put(id, new ListConsumerGroupOffsetsSpec()));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> result = new HashMap<>();
        admin.listConsumerGroupOffsets(specs).all().get().forEach((id, offsets) -> {
            Map<TopicPartition, OffsetAndMetadata> present = new HashMap<>();
            offsets.forEach((tp, oam) -> {
                if (oam != null) {
                    present.put(tp, oam);
                }
            });
            result.put(id, present);
        });
        return result;
    }

    private static Map<TopicPartition, Long> ends(Admin admin, java.util.Collection<TopicPartition> partitions)
            throws ExecutionException, InterruptedException {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(tp -> request.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, Long> result = new HashMap<>();
        admin.listOffsets(request).all().get().forEach((tp, info) -> result.put(tp, info.offset()));
        return result;
    }

    static final Comparator<TopicPartition> TP_ORDER =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    static RuntimeException wrap(Exception e, String what) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (cause instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
            return new UserErrorException(new UserError("Not found", capitalize(what) + " does not exist.", 404));
        }
        return new UserErrorException(KafkaErrors.translate(e, what));
    }

    private static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
