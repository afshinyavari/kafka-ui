package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.serde.Format;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

/**
 * Scans a topic over a time (or offset) range, decodes every record and streams the ones whose decoded key/value
 * match the regexes. Runs on a worker thread; cancelling the subscription stops polling and closes the consumer.
 */
@ApplicationScoped
public class MessageSearchService {

    private static final Duration POLL = Duration.ofMillis(200);
    private static final Duration METADATA_TIMEOUT = TopicPartitions.METADATA_TIMEOUT;
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(5);
    private static final int PROGRESS_EVERY = 1000;
    /** A search that receives nothing from Kafka for this long gives up. */
    static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);

    private final Function<Credentials, Consumer<byte[], byte[]>> consumers;
    private final KafkaUiConfig.Search limits;
    private final Duration idleTimeout;
    private final RecordFilter filter;

    @Inject
    public MessageSearchService(KafkaClients clients, MessageDecoder decoder, KafkaUiConfig config) {
        this(clients::newConsumer, decoder, config.search(), IDLE_TIMEOUT);
    }

    MessageSearchService(Function<Credentials, Consumer<byte[], byte[]>> consumers, MessageDecoder decoder,
            KafkaUiConfig.Search limits) {
        this(consumers, decoder, limits, IDLE_TIMEOUT);
    }

    MessageSearchService(Function<Credentials, Consumer<byte[], byte[]>> consumers, MessageDecoder decoder,
            KafkaUiConfig.Search limits, Duration idleTimeout) {
        this.consumers = consumers;
        this.limits = limits;
        this.idleTimeout = idleTimeout;
        this.filter = new RecordFilter(decoder, limits.regexTimeout());
    }

    public Multi<SearchEvent> search(Credentials credentials, SearchRequest request) {
        return Multi.createFrom().<SearchEvent>emitter(emitter -> run(credentials, request, emitter))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private void run(Credentials credentials, SearchRequest request, MultiEmitter<? super SearchEvent> emitter) {
        Cancellation cancelled = new Cancellation(emitter);
        try (Consumer<byte[], byte[]> consumer = consumers.apply(credentials)) {
            cancelled.attach(consumer);
            if (cancelled.isCancelled()) {
                return;
            }
            Scan scan = new Scan(credentials, request, emitter, cancelled);
            String reason = scan.run(consumer);
            if (!cancelled.isCancelled()) {
                emitter.emit(scan.tally.progress());
                emitter.emit(new SearchEvent.Done(scan.tally.scanned, scan.tally.matched, scan.tally.timeouts, reason));
                emitter.complete();
            }
        } catch (WakeupException e) {
            // cancelled while polling: nothing to report
        } catch (RuntimeException e) {
            if (!cancelled.isCancelled()) {
                emitter.fail(e);
            }
        }
    }

    /** One search's state; lives on a single worker thread. */
    private final class Scan {
        private final Credentials credentials;
        private final SearchRequest request;
        private final MultiEmitter<? super SearchEvent> emitter;
        private final Cancellation cancelled;
        final Tally tally = new Tally();

        Scan(Credentials credentials, SearchRequest request, MultiEmitter<? super SearchEvent> emitter,
                Cancellation cancelled) {
            this.credentials = credentials;
            this.request = request;
            this.emitter = emitter;
            this.cancelled = cancelled;
        }

        String run(Consumer<byte[], byte[]> consumer) {
            Map<TopicPartition, Long> ends = ranges(consumer);
            if (ends.isEmpty()) {
                return SearchEvent.END_OF_RANGE;
            }
            boolean byTime = request.fromOffset().isEmpty();
            long fromMs = request.from().toEpochMilli();
            long toMs = request.to().toEpochMilli();
            long lastProgress = System.nanoTime();
            while (!ends.isEmpty() && !cancelled.isCancelled()) {
                if (System.nanoTime() - lastProgress > idleTimeout.toNanos()) {
                    return SearchEvent.noProgress(idleTimeout);
                }
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL);
                for (ConsumerRecord<byte[], byte[]> r : polled) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    Long end = ends.get(tp);
                    if (end == null || r.offset() >= end) {
                        continue;
                    }
                    if (tally.scanned >= limits.maxScanned()) {
                        return SearchEvent.scanLimit(limits.maxScanned());
                    }
                    if (tally.matched >= limits.maxMatches()) {
                        return SearchEvent.matchLimit(limits.maxMatches());
                    }
                    if (cancelled.isCancelled()) {
                        return SearchEvent.CANCELLED;
                    }
                    tally.scanned++;
                    if (!byTime || (r.timestamp() >= fromMs && r.timestamp() < toMs)) {
                        test(r);
                    }
                    if (tally.scanned % PROGRESS_EVERY == 0) {
                        emitter.emit(tally.progress());
                    }
                }
                if (!polled.isEmpty()) {
                    // idleness is measured from the end of a batch, so slow decoding is not mistaken for a stall
                    lastProgress = System.nanoTime();
                }
                List<TopicPartition> finished = new ArrayList<>();
                ends.forEach((tp, end) -> {
                    if (consumer.position(tp) >= end) {
                        finished.add(tp);
                    }
                });
                if (!finished.isEmpty()) {
                    lastProgress = System.nanoTime();
                    finished.forEach(ends::remove);
                    consumer.pause(finished);
                }
            }
            return cancelled.isCancelled() ? SearchEvent.CANCELLED : SearchEvent.END_OF_RANGE;
        }

        /** Assigns and seeks every partition with a non-empty range; returns the exclusive end offsets. */
        private Map<TopicPartition, Long> ranges(Consumer<byte[], byte[]> consumer) {
            List<TopicPartition> partitions = TopicPartitions.resolve(consumer, request.topic(), request.partitions());
            // assigned before listing offsets, otherwise the consumer warns about unassigned partitions
            consumer.assign(partitions);
            Map<TopicPartition, Long> starts = new HashMap<>();
            if (request.fromOffset().isPresent()) {
                Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(partitions, METADATA_TIMEOUT);
                long from = request.fromOffset().get();
                partitions.forEach(tp -> starts.put(tp, Math.max(from, beginnings.getOrDefault(tp, 0L))));
            } else {
                timesToOffsets(consumer, partitions, request.from()).forEach((tp, o) -> {
                    if (o != null) {
                        starts.put(tp, o);
                    }
                });
            }
            Map<TopicPartition, Long> latest = consumer.endOffsets(partitions, METADATA_TIMEOUT);
            Map<TopicPartition, Long> byTo = timesToOffsets(consumer, partitions, request.to());
            Map<TopicPartition, Long> ends = new HashMap<>();
            starts.forEach((tp, start) -> {
                Long end = byTo.get(tp) != null ? byTo.get(tp) : latest.get(tp);
                if (end != null && start < end) {
                    ends.put(tp, end);
                }
            });
            consumer.assign(ends.keySet());
            ends.keySet().forEach(tp -> consumer.seek(tp, starts.get(tp)));
            return ends;
        }

        private void test(ConsumerRecord<byte[], byte[]> r) {
            tally.record(filter.test(credentials, r, request.keyRegex(), request.valueRegex()), emitter);
        }
    }

    private static Map<TopicPartition, Long> timesToOffsets(Consumer<byte[], byte[]> consumer,
            List<TopicPartition> partitions, Instant time) {
        Map<TopicPartition, Long> query = new HashMap<>();
        partitions.forEach(tp -> query.put(tp, time.toEpochMilli()));
        Map<TopicPartition, Long> result = new HashMap<>();
        consumer.offsetsForTimes(query, METADATA_TIMEOUT).forEach((tp, o) -> result.put(tp, o == null ? null : o.offset()));
        return result;
    }

    /**
     * Reads exactly one record. {@code keyAs}/{@code valueAs} null decode automatically. Empty when the offset is
     * outside the partition or no longer exists (compaction).
     */
    public Optional<MessageView> fetch(Credentials credentials, String topic, int partition, long offset,
            Format keyAs, Format valueAs) {
        TopicPartition tp = new TopicPartition(topic, partition);
        try (Consumer<byte[], byte[]> consumer = consumers.apply(credentials)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, METADATA_TIMEOUT);
            if (infos == null || infos.stream().noneMatch(p -> p.partition() == partition)) {
                return Optional.empty();
            }
            List<TopicPartition> one = List.of(tp);
            consumer.assign(one);
            long begin = consumer.beginningOffsets(one, METADATA_TIMEOUT).getOrDefault(tp, 0L);
            long end = consumer.endOffsets(one, METADATA_TIMEOUT).getOrDefault(tp, 0L);
            if (offset < begin || offset >= end) {
                return Optional.empty();
            }
            consumer.seek(tp, offset);
            long deadline = System.nanoTime() + FETCH_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(POLL)) {
                    if (r.offset() == offset) {
                        return Optional.of(filter.view(credentials, r, keyAs, valueAs));
                    }
                    if (r.offset() > offset) {
                        return Optional.empty();
                    }
                }
                if (consumer.position(tp) > offset) {
                    return Optional.empty();
                }
            }
            return Optional.empty();
        }
    }
}
