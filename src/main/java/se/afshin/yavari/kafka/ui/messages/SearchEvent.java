package se.afshin.yavari.kafka.ui.messages;

/** What a running search reports: matching messages, progress, and finally why it stopped. */
public sealed interface SearchEvent permits SearchEvent.Match, SearchEvent.Progress, SearchEvent.Done,
        SearchEvent.Expired {

    String END_OF_RANGE = "end of range";
    String CANCELLED = "cancelled";

    static String scanLimit(int max) {
        return "scan limit reached (" + max + " messages)";
    }

    static String matchLimit(int max) {
        return "match limit reached (" + max + ")";
    }

    static String noProgress(java.time.Duration idle) {
        return "no progress from Kafka for " + idle.toSeconds() + " s";
    }

    record Match(MessageView message) implements SearchEvent {}

    record Progress(long scanned, long matched, long regexTimeouts) implements SearchEvent {}

    /** The user's token expired: the stream ended early with the counts so far. */
    record Expired(long scanned, long matched, long regexTimeouts) implements SearchEvent {}

    record Done(long scanned, long matched, long regexTimeouts, String stopReason) implements SearchEvent {}
}
