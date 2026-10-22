package se.afshin.yavari.kafka.ui.topics;

/** One topic setting; the value of sensitive entries is already masked. */
public record ConfigRow(String name, String value, boolean isDefault) {
}
