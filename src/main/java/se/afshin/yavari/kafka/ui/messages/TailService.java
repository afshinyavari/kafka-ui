package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

/**
 * Follows a topic from its current end and streams the records whose decoded key/value match the regexes. Emits
 * {@link SearchEvent.Match} and periodic {@link SearchEvent.Progress} (the first one as soon as the consumer is
 * positioned at the end); never {@code Done}. Runs on a worker thread until the subscription is cancelled.
 */
@ApplicationScoped
public class TailService {

    private static final Duration POLL = Duration.ofMillis(500);
    private static final Duration METADATA_TIMEOUT = Duration.ofSeconds(15);
    private static final long PROGRESS_EVERY_NANOS = Duration.ofSeconds(1).toNanos();

    private final Function<Credentials, Consumer<byte[], byte[]>> consumers;
    private final RecordFilter filter;

    @Inject
    public TailService(KafkaClients clients, MessageDecoder decoder, KafkaUiConfig config) {
        this(clients::newConsumer, decoder, config.search().regexTimeout());
    }

    TailService(Function<Credentials, Consumer<byte[], byte[]>> consumers, MessageDecoder decoder,
            Duration regexTimeout) {
        this.consumers = consumers;
        this.filter = new RecordFilter(decoder, regexTimeout);
    }

    /** Empty {@code partitions} means all. */
    public Multi<SearchEvent> tail(Credentials credentials, String topic, Set<Integer> partitions,
            Optional<Pattern> keyRegex, Optional<Pattern> valueRegex) {
        return Multi.createFrom().<SearchEvent>emitter(
                        emitter -> run(credentials, topic, partitions, keyRegex, valueRegex, emitter))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private void run(Credentials credentials, String topic, Set<Integer> partitions, Optional<Pattern> keyRegex,
            Optional<Pattern> valueRegex, MultiEmitter<? super SearchEvent> emitter) {
        Cancellation cancelled = new Cancellation(emitter);
        try (Consumer<byte[], byte[]> consumer = consumers.apply(credentials)) {
            cancelled.attach(consumer);
            if (cancelled.isCancelled()) {
                return;
            }
            List<TopicPartition> assigned = partitions(consumer, topic, partitions);
            consumer.assign(assigned);
            consumer.seekToEnd(assigned);
            assigned.forEach(consumer::position); // seeking is lazy: resolve the end now, before reporting readiness
            long scanned = 0;
            long matched = 0;
            long timeouts = 0;
            emitter.emit(new SearchEvent.Progress(0, 0, 0));
            long lastReport = System.nanoTime();
            long reported = 0;
            while (!cancelled.isCancelled()) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(POLL)) {
                    if (cancelled.isCancelled()) {
                        return;
                    }
                    scanned++;
                    RecordFilter.Verdict v = filter.test(credentials, r, keyRegex, valueRegex);
                    switch (v.result()) {
                        case TIMEOUT -> timeouts++;
                        case MATCH -> {
                            matched++;
                            emitter.emit(new SearchEvent.Match(v.message()));
                        }
                        case NO_MATCH -> { }
                    }
                }
                if (scanned != reported && System.nanoTime() - lastReport >= PROGRESS_EVERY_NANOS) {
                    emitter.emit(new SearchEvent.Progress(scanned, matched, timeouts));
                    reported = scanned;
                    lastReport = System.nanoTime();
                }
            }
        } catch (WakeupException e) {
            // cancelled while polling: nothing to report
        } catch (RuntimeException e) {
            if (!cancelled.isCancelled()) {
                emitter.fail(e);
            }
        }
    }

    private static List<TopicPartition> partitions(Consumer<byte[], byte[]> consumer, String topic,
            Set<Integer> wanted) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic, METADATA_TIMEOUT);
        if (infos == null || infos.isEmpty()) {
            throw new UnknownTopicOrPartitionException("Topic " + topic + " not found");
        }
        return infos.stream()
                .map(PartitionInfo::partition)
                .filter(p -> wanted.isEmpty() || wanted.contains(p))
                .sorted()
                .map(p -> new TopicPartition(topic, p))
                .toList();
    }
}
