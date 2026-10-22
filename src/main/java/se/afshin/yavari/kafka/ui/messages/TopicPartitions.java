package se.afshin.yavari.kafka.ui.messages;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;

/** Resolves the partitions a search or tail reads; shared so both validate the same way. */
final class TopicPartitions {

    static final Duration METADATA_TIMEOUT = Duration.ofSeconds(15);

    private TopicPartitions() {}

    /** Empty {@code wanted} means all. A wanted partition the topic does not have is a user error. */
    static List<TopicPartition> resolve(Consumer<byte[], byte[]> consumer, String topic, Set<Integer> wanted) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic, METADATA_TIMEOUT);
        if (infos == null || infos.isEmpty()) {
            throw new UnknownTopicOrPartitionException("Topic " + topic + " not found");
        }
        Set<Integer> existing = Set.copyOf(infos.stream().map(PartitionInfo::partition).toList());
        wanted.stream().sorted().filter(p -> !existing.contains(p)).findFirst().ifPresent(p -> {
            throw new UserErrorException(new UserError("Invalid search",
                    "Partition " + p + " does not exist in topic " + topic + ".", 400));
        });
        return existing.stream()
                .filter(p -> wanted.isEmpty() || wanted.contains(p))
                .sorted()
                .map(p -> new TopicPartition(topic, p))
                .toList();
    }
}
