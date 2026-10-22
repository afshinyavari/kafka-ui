package se.afshin.yavari.kafka.ui.messages;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A message search. Empty {@code partitions} means all. With {@code fromOffset} the search starts at that offset in
 * every partition instead of at {@code from}; {@code to} always bounds the end. Both regexes given: both must match.
 */
public record SearchRequest(String topic, Set<Integer> partitions, Instant from, Instant to,
        Optional<Long> fromOffset, Optional<Pattern> keyRegex, Optional<Pattern> valueRegex) {}
