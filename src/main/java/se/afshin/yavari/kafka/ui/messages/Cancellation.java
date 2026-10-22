package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.subscription.MultiEmitter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Cancellation of a streaming consumer run. Cancelling the subscription wakes the consumer so a blocking poll
 * returns; the consumer itself is closed by the polling thread (it is not thread safe).
 */
final class Cancellation {

    private final MultiEmitter<?> emitter;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicReference<Consumer<byte[], byte[]>> current = new AtomicReference<>();

    Cancellation(MultiEmitter<?> emitter) {
        this.emitter = emitter;
        emitter.onTermination(() -> {
            cancelled.set(true);
            Consumer<byte[], byte[]> c = current.get();
            if (c != null) {
                c.wakeup();
            }
        });
    }

    void attach(Consumer<byte[], byte[]> consumer) {
        current.set(consumer);
    }

    /** Also true when the subscription was cancelled before the termination callback was registered. */
    boolean isCancelled() {
        return cancelled.get() || emitter.isCancelled();
    }
}
