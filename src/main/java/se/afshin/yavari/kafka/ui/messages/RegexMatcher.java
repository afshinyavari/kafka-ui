package se.afshin.yavari.kafka.ui.messages;

import java.time.Duration;
import java.util.regex.Pattern;

/** Regex find with a deadline, so a pathological pattern cannot hang a search. */
public final class RegexMatcher {

    public enum Result {
        MATCH,
        NO_MATCH,
        TIMEOUT
    }

    private RegexMatcher() {
    }

    public static Result find(Pattern p, String text, Duration timeout) {
        try {
            return p.matcher(new Deadline(text, System.nanoTime() + timeout.toNanos())).find()
                    ? Result.MATCH
                    : Result.NO_MATCH;
        } catch (TimedOut | StackOverflowError e) {
            return Result.TIMEOUT;
        }
    }

    private static final class TimedOut extends RuntimeException {
        TimedOut() {
            super(null, null, false, false);
        }
    }

    /** Text whose {@code charAt} throws once the deadline has passed. */
    private static final class Deadline implements CharSequence {
        private static final int CHECK_EVERY = 1024;

        private final CharSequence text;
        private final long deadline;
        private int calls;

        Deadline(CharSequence text, long deadline) {
            this.text = text;
            this.deadline = deadline;
        }

        @Override
        public char charAt(int index) {
            if (++calls % CHECK_EVERY == 0 && System.nanoTime() - deadline > 0) {
                throw new TimedOut();
            }
            return text.charAt(index);
        }

        @Override
        public int length() {
            return text.length();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new Deadline(text.subSequence(start, end), deadline);
        }

        @Override
        public String toString() {
            return text.toString();
        }
    }
}
