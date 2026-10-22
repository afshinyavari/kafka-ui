package se.afshin.yavari.kafka.ui.kafka;

/** A plain-language error that is safe to show to end users. */
public record UserError(String title, String message, int httpStatus) {
}
