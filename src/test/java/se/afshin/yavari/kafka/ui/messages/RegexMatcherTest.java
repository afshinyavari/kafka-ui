package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class RegexMatcherTest {

    private static final Duration TIMEOUT = Duration.ofMillis(100);

    @Test
    void match() {
        assertThat(RegexMatcher.find(Pattern.compile("o-4\\d"), "{\"id\":\"o-42\"}", TIMEOUT))
                .isEqualTo(RegexMatcher.Result.MATCH);
    }

    @Test
    void noMatch() {
        assertThat(RegexMatcher.find(Pattern.compile("o-4\\d"), "{\"id\":\"o-13\"}", TIMEOUT))
                .isEqualTo(RegexMatcher.Result.NO_MATCH);
    }

    @Test
    void catastrophicBacktrackingTimesOut() {
        long start = System.nanoTime();
        var r = RegexMatcher.find(Pattern.compile("(a+)+$"), "a".repeat(50_000) + "!", TIMEOUT);
        assertThat(r).isEqualTo(RegexMatcher.Result.TIMEOUT);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
    }
}
