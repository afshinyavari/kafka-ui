package se.afshin.yavari.kafka.ui.topics;

/** One topic setting; overridden means set on the topic itself. The value of sensitive entries is already masked. */
public record ConfigRow(String name, String value, boolean overridden) {
}
