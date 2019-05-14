package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean isTombstone;

    static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    public Value(@NotNull ByteBuffer data, final long timestamp, final boolean isTombstone) {
        this.data = data;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
    }

    public static Value of(@NotNull ByteBuffer data) {
        return new Value(data, System.currentTimeMillis(), false);
    }

    public static Value ofTombstone() {
        return new Value(EMPTY_BUFFER, System.currentTimeMillis(), true);
    }


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
