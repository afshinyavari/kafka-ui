package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CancellationTest {

    @Test
    void seesCancellationThatHappenedBeforeItWasCreated() {
        AtomicReference<MultiEmitter<? super String>> captured = new AtomicReference<>();
        var subscriber = Multi.createFrom().<String>emitter(captured::set)
                .subscribe().withSubscriber(AssertSubscriber.<String>create(1));
        subscriber.cancel(); // before any termination callback was registered

        assertThat(new Cancellation(captured.get()).isCancelled()).isTrue();
    }

    @Test
    void notCancelledWhileSubscribed() {
        AtomicReference<MultiEmitter<? super String>> captured = new AtomicReference<>();
        Multi.createFrom().<String>emitter(captured::set)
                .subscribe().withSubscriber(AssertSubscriber.<String>create(1));

        assertThat(new Cancellation(captured.get()).isCancelled()).isFalse();
    }
}
