package se.afshin.yavari.kafka.ui.messages;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;

/** The search form as typed (times are UTC {@code datetime-local} values); blanks are filled with defaults. */
public record SearchForm(List<Integer> partitions, String from, String to, boolean byOffset, String offset,
        String keyRegex, String valueRegex) {

    static final DateTimeFormatter INPUT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public static SearchForm of(List<Integer> partitions, String from, String to, boolean byOffset, String offset,
            String keyRegex, String valueRegex, Duration defaultRange, Instant now) {
        Instant end = now.truncatedTo(ChronoUnit.SECONDS);
        return new SearchForm(partitions == null ? List.of() : List.copyOf(partitions),
                blank(from) ? format(end.minus(defaultRange)) : from.strip(),
                blank(to) ? format(end) : to.strip(),
                byOffset,
                offset == null ? "" : offset.strip(),
                keyRegex == null ? "" : keyRegex,
                valueRegex == null ? "" : valueRegex);
    }

    public boolean selected(int partition) {
        return partitions.contains(partition);
    }

    /** Validates and converts; invalid input becomes a {@link UserErrorException} with a plain-language message. */
    public SearchRequest toRequest(String topic) {
        Instant fromTime = byOffset ? Instant.EPOCH : parseTime("From", from);
        Instant toTime = parseTime("To", to);
        if (!byOffset && !fromTime.isBefore(toTime)) {
            throw invalid("From must be before To.");
        }
        Optional<Long> startOffset = Optional.empty();
        if (byOffset) {
            try {
                long o = Long.parseLong(offset);
                if (o < 0) {
                    throw new NumberFormatException();
                }
                startOffset = Optional.of(o);
            } catch (NumberFormatException e) {
                throw invalid("Enter a start offset of 0 or more.");
            }
        }
        return new SearchRequest(topic, Set.copyOf(partitions), fromTime, toTime, startOffset,
                regex("key", keyRegex), regex("value", valueRegex));
    }

    /** Query string that reproduces this form, for the event stream URL. */
    public String query() {
        StringJoiner q = new StringJoiner("&");
        partitions.forEach(p -> q.add("partition=" + p));
        q.add("from=" + encode(from));
        q.add("to=" + encode(to));
        if (byOffset) {
            q.add("byOffset=true");
            q.add("offset=" + encode(offset));
        }
        if (!keyRegex.isEmpty()) {
            q.add("keyRegex=" + encode(keyRegex));
        }
        if (!valueRegex.isEmpty()) {
            q.add("valueRegex=" + encode(valueRegex));
        }
        return q.toString();
    }

    private static Optional<Pattern> regex(String what, String text) {
        if (text.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Pattern.compile(text));
        } catch (PatternSyntaxException e) {
            String where = e.getIndex() >= 0 ? " at position " + (e.getIndex() + 1) : "";
            throw invalid("Invalid " + what + " regex: " + e.getDescription() + where + ".");
        }
    }

    private static Instant parseTime(String label, String text) {
        try {
            return LocalDateTime.parse(text).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw invalid("Invalid " + label + " time \"" + text + "\": use yyyy-MM-ddTHH:mm (UTC).");
        }
    }

    private static UserErrorException invalid(String message) {
        return new UserErrorException(new UserError("Invalid search", message, 400));
    }

    private static String format(Instant t) {
        return INPUT.format(t.atOffset(ZoneOffset.UTC));
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
