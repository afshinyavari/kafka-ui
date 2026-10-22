package se.afshin.yavari.kafka.ui.groups;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** The one group write operation: move a stopped group's committed offsets. Kafka decides who may do it. */
@ApplicationScoped
public class ResetOffsetsService {

    @Inject KafkaClients clients;

    /** @return the new committed offset per partition, ordered by partition */
    public Map<TopicPartition, Long> reset(Credentials credentials, ResetRequest req) {
        String what = "consumer group " + req.groupId();
        try {
            Admin admin = clients.admin(credentials);
            var state = admin.describeConsumerGroups(java.util.List.of(req.groupId())).all().get()
                    .get(req.groupId()).groupState();
            if (state != GroupState.EMPTY && state != GroupState.DEAD) {
                throw new UserErrorException(new UserError("Group is active",
                        "Stop all consumers in group " + req.groupId() + " before resetting offsets.", 409));
            }
            Set<TopicPartition> targets = targets(admin, req);
            Map<TopicPartition, Long> starts = offsets(admin, targets, OffsetSpec.earliest());
            Map<TopicPartition, Long> ends = offsets(admin, targets, OffsetSpec.latest());
            Map<TopicPartition, Long> chosen = switch (req.to()) {
                case EARLIEST -> starts;
                case LATEST -> ends;
                case TIMESTAMP -> {
                    long ms = req.timestamp().orElseThrow(() -> invalid("Pick a date and time.")).toEpochMilli();
                    Map<TopicPartition, Long> found = offsets(admin, targets, OffsetSpec.forTimestamp(ms));
                    Map<TopicPartition, Long> out = new HashMap<>();
                    targets.forEach(tp -> {
                        long o = found.get(tp);
                        out.put(tp, o < 0 ? ends.get(tp) : o);
                    });
                    yield out;
                }
                case OFFSET -> {
                    long wanted = req.offset().orElseThrow(() -> invalid("Enter an offset."));
                    Map<TopicPartition, Long> out = new HashMap<>();
                    targets.forEach(tp -> out.put(tp, Math.max(starts.get(tp), Math.min(ends.get(tp), wanted))));
                    yield out;
                }
            };
            Map<TopicPartition, OffsetAndMetadata> alter = new HashMap<>();
            chosen.forEach((tp, o) -> alter.put(tp, new OffsetAndMetadata(o)));
            admin.alterConsumerGroupOffsets(req.groupId(), alter).all().get();
            Map<TopicPartition, Long> sorted = new LinkedHashMap<>();
            chosen.keySet().stream().sorted(GroupService.TP_ORDER).forEach(tp -> sorted.put(tp, chosen.get(tp)));
            return sorted;
        } catch (UserErrorException e) {
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                throw invalid("Topic " + req.topic() + " or one of the requested partitions does not exist.");
            }
            throw GroupService.wrap(e, what);
        }
    }

    private static Set<TopicPartition> targets(Admin admin, ResetRequest req) throws Exception {
        Set<TopicPartition> committed = admin.listConsumerGroupOffsets(
                Map.of(req.groupId(), new ListConsumerGroupOffsetsSpec())).all().get().get(req.groupId())
                .entrySet().stream()
                .filter(e -> e.getValue() != null && e.getKey().topic().equals(req.topic()))
                .map(Map.Entry::getKey).collect(Collectors.toSet());
        Set<TopicPartition> targets = req.partitions().isEmpty()
                ? committed
                : req.partitions().stream().map(p -> new TopicPartition(req.topic(), p)).collect(Collectors.toSet());
        if (targets.isEmpty()) {
            throw invalid("Group " + req.groupId() + " has no committed offsets on topic " + req.topic() + ".");
        }
        return targets;
    }

    private static Map<TopicPartition, Long> offsets(Admin admin, Set<TopicPartition> partitions, OffsetSpec spec)
            throws Exception {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(tp -> request.put(tp, spec));
        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResultInfo> e : admin.listOffsets(request).all().get().entrySet()) {
            result.put(e.getKey(), e.getValue().offset());
        }
        return result;
    }

    private static UserErrorException invalid(String message) {
        return new UserErrorException(new UserError("Invalid request", message, 400));
    }
}
