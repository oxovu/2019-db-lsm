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

    public Row(@NotNull ByteBuffer key, ByteBuffer value, long timestamp) {
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

    public static Row of(@NotNull ByteBuffer key, ByteBuffer value) {
        return new Row(key, value, System.currentTimeMillis());
    }

    public static Row ofData(@NotNull ByteBuffer key, ByteBuffer value, long tombstone) {
        return new Row(key, value, tombstone);
    }

    public boolean isTombstone() {
        return this.timestamp < 0;
    }

    public static Row ofTombstone(@NotNull ByteBuffer key) {
        return new Row(key, null, System.currentTimeMillis() * -1);
    }

    public int getSize() {
        return 2 * Integer.BYTES + Long.BYTES + key.remaining() + value.remaining();
    }
}
