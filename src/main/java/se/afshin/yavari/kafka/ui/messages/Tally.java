package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.subscription.MultiEmitter;

/** Running counts of a search or tail; applies a filter verdict and emits the match. Single-threaded. */
final class Tally {
    long scanned;
    long matched;
    long timeouts;

    /** Counts the verdict of one tested record (the caller counts {@code scanned}); emits it when it matched. */
    void record(RecordFilter.Verdict v, MultiEmitter<? super SearchEvent> emitter) {
        switch (v.result()) {
            case TIMEOUT -> timeouts++;
            case MATCH -> {
                matched++;
                emitter.emit(new SearchEvent.Match(v.message()));
            }
            case NO_MATCH -> { }
        }
    }

    SearchEvent.Progress progress() {
        return new SearchEvent.Progress(scanned, matched, timeouts);
    }
}
