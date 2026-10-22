package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

/**
 * Follows a topic from its current end and streams the records whose decoded key/value match the regexes. Emits
 * {@link SearchEvent.Match} and periodic {@link SearchEvent.Progress} (the first one as soon as the consumer is
 * positioned at the end); never {@code Done}. Ends with {@link SearchEvent.Expired} when the bearer token expires. Runs on a worker thread until the subscription is cancelled.
 */
@ApplicationScoped
public class TailService {

    private static final Duration POLL = Duration.ofMillis(500);
    private static final long PROGRESS_EVERY_NANOS = Duration.ofSeconds(1).toNanos();

    private final Function<Credentials, Consumer<byte[], byte[]>> consumers;
    private final RecordFilter filter;
    private final Clock clock;

    @Inject
    public TailService(KafkaClients clients, MessageDecoder decoder, KafkaUiConfig config) {
        this(clients::newConsumer, decoder, config.search().regexTimeout());
    }

    TailService(Function<Credentials, Consumer<byte[], byte[]>> consumers, MessageDecoder decoder,
            Duration regexTimeout) {
        this(consumers, decoder, regexTimeout, Clock.systemUTC());
    }

    TailService(Function<Credentials, Consumer<byte[], byte[]>> consumers, MessageDecoder decoder,
            Duration regexTimeout, Clock clock) {
        this.clock = clock;
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
            List<TopicPartition> assigned = TopicPartitions.resolve(consumer, topic, partitions);
            consumer.assign(assigned);
            consumer.seekToEnd(assigned);
            assigned.forEach(consumer::position); // seeking is lazy: resolve the end now, before reporting readiness
            Tally tally = new Tally();
            emitter.emit(new SearchEvent.Progress(0, 0, 0));
            long lastReport = System.nanoTime();
            long reported = 0;
            while (!cancelled.isCancelled()) {
                if (credentials.expiredAt(clock.instant())) {
                    emitter.emit(new SearchEvent.Expired(tally.scanned, tally.matched, tally.timeouts));
                    emitter.complete();
                    return;
                }
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(POLL)) {
                    if (cancelled.isCancelled()) {
                        return;
                    }
                    tally.scanned++;
                    tally.record(filter.test(credentials, r, keyRegex, valueRegex), emitter);
                }
                if (tally.scanned != reported && System.nanoTime() - lastReport >= PROGRESS_EVERY_NANOS) {
                    emitter.emit(tally.progress());
                    reported = tally.scanned;
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
}
