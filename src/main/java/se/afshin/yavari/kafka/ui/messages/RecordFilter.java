package se.afshin.yavari.kafka.ui.messages;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.serde.Format;
import se.afshin.yavari.kafka.ui.serde.MessageDecoder;

/** Turns consumer records into {@link MessageView}s and applies the key/value regexes; shared by search and tail. */
final class RecordFilter {

    /** {@code message} is set for every outcome; only a {@link RegexMatcher.Result#MATCH} should be shown. */
    record Verdict(RegexMatcher.Result result, MessageView message) {}

    private final MessageDecoder decoder;
    private final Duration regexTimeout;

    RecordFilter(MessageDecoder decoder, Duration regexTimeout) {
        this.decoder = decoder;
        this.regexTimeout = regexTimeout;
    }

    MessageView view(Credentials credentials, ConsumerRecord<byte[], byte[]> r, Format keyAs, Format valueAs) {
        List<HeaderView> headers = new ArrayList<>();
        for (Header h : r.headers()) {
            headers.add(HeaderView.of(h));
        }
        return new MessageView(r.partition(), r.offset(), Instant.ofEpochMilli(r.timestamp()),
                decoder.decodeAs(credentials, r.key(), r.headers(), true, keyAs),
                decoder.decodeAs(credentials, r.value(), r.headers(), false, valueAs),
                List.copyOf(headers));
    }

    /** Decodes automatically; both regexes (when given) must match the decoded text. A timeout counts as no match. */
    Verdict test(Credentials credentials, ConsumerRecord<byte[], byte[]> r, Optional<Pattern> keyRegex,
            Optional<Pattern> valueRegex) {
        MessageView m = view(credentials, r, null, null);
        RegexMatcher.Result key = matches(keyRegex, m.key().text());
        RegexMatcher.Result value = key == RegexMatcher.Result.MATCH
                ? matches(valueRegex, m.value().text())
                : RegexMatcher.Result.NO_MATCH;
        if (key == RegexMatcher.Result.TIMEOUT || value == RegexMatcher.Result.TIMEOUT) {
            return new Verdict(RegexMatcher.Result.TIMEOUT, m);
        }
        return new Verdict(value, m);
    }

    private RegexMatcher.Result matches(Optional<Pattern> regex, String text) {
        return regex.map(p -> RegexMatcher.find(p, text, regexTimeout)).orElse(RegexMatcher.Result.MATCH);
    }
}
