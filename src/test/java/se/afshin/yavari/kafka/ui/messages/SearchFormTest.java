package se.afshin.yavari.kafka.ui.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SearchFormTest {

    private static final Instant NOW = Instant.parse("2026-10-22T12:00:05.700Z");

    private static SearchForm form(String from, String to) {
        return SearchForm.of(null, from, to, false, null, null, null, Duration.ofHours(1), NOW);
    }

    @Test
    void emptyToStaysEmptyAndFromDefaultsToRangeBeforeNow() {
        var form = form(null, "  ");
        assertThat(form.to()).isEmpty();
        assertThat(form.from()).isEqualTo("2026-10-22T11:00:05");
        assertThat(form.query()).doesNotContain("to=");
    }

    @Test
    void emptyToMeansEndOfCurrentSecondAtSearchTime() {
        var request = form(null, "").toRequest("t", Instant.parse("2026-10-22T12:30:09.100Z"));
        assertThat(request.to()).isEqualTo(Instant.parse("2026-10-22T12:30:10Z"));
    }

    @Test
    void explicitToIsKept() {
        var form = form(null, "2026-10-22T11:30:00");
        assertThat(form.query()).contains("to=2026-10-22T11%3A30%3A00");
        assertThat(form.toRequest("t", NOW).to()).isEqualTo(Instant.parse("2026-10-22T11:30:00Z"));
    }
}
