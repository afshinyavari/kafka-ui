package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;

class StreamLimiterTest {

    private static final Credentials ALICE = new Credentials.Bearer("a", "alice", Instant.now().plusSeconds(600));
    private static final Credentials BOB = new Credentials.Bearer("b", "bob", Instant.now().plusSeconds(600));
    private static final Credentials CAROL = new Credentials.Bearer("c", "carol", Instant.now().plusSeconds(600));

    private static AssertSubscriber<Integer> open(StreamLimiter limiter, Credentials who) {
        return limiter.limit(who, Multi.createFrom().<Integer>nothing())
                .subscribe().withSubscriber(AssertSubscriber.create(1));
    }

    private static void assertTooMany(AssertSubscriber<Integer> s) {
        s.assertFailedWith(UserErrorException.class, StreamLimiter.TOO_MANY);
        assertThat(((UserErrorException) s.getFailure()).error().httpStatus()).isEqualTo(429);
    }

    @Test
    void capsStreamsPerIdentity() {
        var limiter = new StreamLimiter(10, 2);
        var a1 = open(limiter, ALICE);
        var a2 = open(limiter, ALICE);
        assertTooMany(open(limiter, ALICE));
        open(limiter, BOB).assertNotTerminated();
        a1.cancel();
        open(limiter, ALICE).assertNotTerminated();
        a2.assertNotTerminated();
    }

    @Test
    void sharedMtlsIdentityIsOnlyCappedOverall() {
        var limiter = new StreamLimiter(4, 1);
        var mtls = new Credentials.Mtls();
        open(limiter, mtls).assertNotTerminated();
        open(limiter, mtls).assertNotTerminated();
        open(limiter, mtls).assertNotTerminated();
        open(limiter, ALICE).assertNotTerminated();
        assertTooMany(open(limiter, mtls));
    }

    @Test
    void capsStreamsOverall() {
        var limiter = new StreamLimiter(2, 3);
        open(limiter, ALICE);
        var bob = open(limiter, BOB);
        assertTooMany(open(limiter, CAROL));
        bob.cancel();
        open(limiter, CAROL).assertNotTerminated();
    }

    @Test
    void completedAndFailedStreamsFreeTheirSlot() {
        var limiter = new StreamLimiter(10, 1);
        limiter.limit(ALICE, Multi.createFrom().items(1, 2)).subscribe().withSubscriber(AssertSubscriber.create(5))
                .assertCompleted();
        limiter.limit(ALICE, Multi.createFrom().<Integer>failure(new IllegalStateException("x")))
                .subscribe().withSubscriber(AssertSubscriber.create(5)).assertFailedWith(IllegalStateException.class);
        open(limiter, ALICE).assertNotTerminated();
    }

    @Test
    void streamsRunOnABoundedDedicatedPool() throws Exception {
        var limiter = new StreamLimiter(4, 3);
        assertThat(limiter.poolSize()).isEqualTo(4);
        CountDownLatch ran = new CountDownLatch(1);
        String[] thread = new String[1];
        limiter.executor().execute(() -> {
            thread[0] = Thread.currentThread().getName();
            ran.countDown();
        });
        assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(thread[0]).startsWith("kafka-ui-stream-");
        limiter.shutdown();
    }

    @Test
    void searchUsesTheLimiterPool() {
        var limiter = new StreamLimiter(4, 3);
        String[] thread = new String[1];
        var s = Multi.createFrom().<String>emitter(e -> {
                    thread[0] = Thread.currentThread().getName();
                    e.complete();
                }).runSubscriptionOn(limiter.executor())
                .subscribe().withSubscriber(AssertSubscriber.create(1));
        s.awaitCompletion(Duration.ofSeconds(5));
        assertThat(thread[0]).startsWith("kafka-ui-stream-");
    }
}
