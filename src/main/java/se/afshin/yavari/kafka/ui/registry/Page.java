package se.afshin.yavari.kafka.ui.registry;

import java.util.List;

public record Page<T>(List<T> items, long total, int offset, int limit) {

    public boolean hasPrevious() {
        return offset > 0;
    }

    public boolean hasNext() {
        return offset + limit < total;
    }

    public int previousOffset() {
        return Math.max(0, offset - limit);
    }

    public int nextOffset() {
        return offset + limit;
    }
}
