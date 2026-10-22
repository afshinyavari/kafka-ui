package se.afshin.yavari.kafka.ui.security;

/** Authentication mode of the UI, selected with {@code kafka-ui.mode}. */
public enum Mode {
    MTLS,
    OIDC
}
