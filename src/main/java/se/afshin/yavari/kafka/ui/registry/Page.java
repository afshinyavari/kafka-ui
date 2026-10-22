package se.afshin.yavari.kafka.ui.registry;

import java.util.List;

public record Page<T>(List<T> items, long total, int offset, int limit) {}
