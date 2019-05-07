package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Row {

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timestamp;

    public static final Comparator<Row> comparator = Comparator.comparing(Row::getKey).thenComparing(Row::getValue)
            .thenComparing(Row::getTimestamp);

    /**
     * @param key
     * @param value
     * @param timestamp
     */
    public Row(@NotNull final ByteBuffer key, final ByteBuffer value, final long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public static Row of(@NotNull final ByteBuffer key, final ByteBuffer value) {
        return new Row(key, value, System.currentTimeMillis());
    }

    public static Row ofData(@NotNull final ByteBuffer key, final ByteBuffer value, final long tombstone) {
        return new Row(key, value, tombstone);
    }

    public boolean isTombstone() {
        return this.timestamp < 0;
    }

    public static Row ofTombstone(@NotNull final ByteBuffer key) {
        return new Row(key, null, System.currentTimeMillis() * -1);
    }

    public int getSize() {
        return 2 * Integer.BYTES + Long.BYTES + key.remaining() + value.remaining();
    }
}
