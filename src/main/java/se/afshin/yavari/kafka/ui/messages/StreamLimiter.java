package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.Multi;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

/**
 * Bounds searches and tails: they run on a dedicated pool of {@code maxConcurrent} threads (not the shared worker
 * pool), and each identity may run at most {@code maxPerIdentity} at once. A stream over either limit fails right
 * away with {@link #TOO_MANY}; a slot is freed when the stream completes, fails or is cancelled.
 */
@ApplicationScoped
public class StreamLimiter {

    public static final String TOO_MANY = "Too many running searches – stop one and try again.";

    private final int maxConcurrent;
    private final int maxPerIdentity;
    private final ThreadPoolExecutor executor;
    private final Map<String, Integer> running = new HashMap<>();
    private int total;

    @Inject
    public StreamLimiter(KafkaUiConfig config) {
        this(config.streams().maxConcurrent(), config.streams().maxPerUser());
    }

    StreamLimiter(int maxConcurrent, int maxPerIdentity) {
        this.maxConcurrent = maxConcurrent;
        this.maxPerIdentity = maxPerIdentity;
        AtomicInteger n = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(Math.max(1, maxConcurrent), Math.max(1, maxConcurrent), 60,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
                    Thread t = new Thread(r, "kafka-ui-stream-" + n.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });
        this.executor.allowCoreThreadTimeOut(true);
    }

    /** The pool searches and tails run on. */
    public Executor executor() {
        return executor;
    }

    int poolSize() {
        return executor.getMaximumPoolSize();
    }

    /** Takes a slot when {@code stream} is subscribed and frees it when the stream terminates. */
    public <T> Multi<T> limit(Credentials credentials, Multi<T> stream) {
        String scope = credentials.cacheScope();
        return Multi.createFrom().deferred(() -> {
            if (!acquire(scope)) {
                return Multi.createFrom().failure(
                        new UserErrorException(new UserError("Too many searches", TOO_MANY, 429)));
            }
            AtomicBoolean released = new AtomicBoolean();
            return stream.onTermination().invoke(() -> {
                if (released.compareAndSet(false, true)) {
                    release(scope);
                }
            });
        });
    }

    private synchronized boolean acquire(String scope) {
        int mine = running.getOrDefault(scope, 0);
        if (total >= maxConcurrent || mine >= maxPerIdentity) {
            return false;
        }
        running.put(scope, mine + 1);
        total++;
        return true;
    }

    private synchronized void release(String scope) {
        running.computeIfPresent(scope, (k, v) -> v <= 1 ? null : v - 1);
        total--;
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
