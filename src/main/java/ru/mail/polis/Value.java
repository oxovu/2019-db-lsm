package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean isTombstone;

    static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    /**
     * Class for storing data in row.
     *
     * @param data what to record
     * @param timestamp when the record was created
     * @param isTombstone if record was deleted
     */
    public Value(@NotNull final ByteBuffer data, final long timestamp, final boolean isTombstone) {
        this.data = data;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
    }

    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(data, System.currentTimeMillis(), false);
    }

    public static Value ofTombstone() {
        return new Value(EMPTY, System.currentTimeMillis(), true);
    }

    /**
     * Counts necessary size to store value.
     *
     * @return necessary size to store value
     */
    public int getSize() {
        if (isTombstone) {
            return Long.BYTES;
        } else {
            return Integer.BYTES + Long.BYTES + data.remaining();
        }
    }

    public int compareTo(@NotNull final Value v) {
        return Long.compare(v.timestamp, timestamp);
    }

    public ByteBuffer getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isTombstone() {
        return isTombstone;
    }
}
