package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Row {

    private final ByteBuffer key;
    private final Value value;

    public static final Comparator<Row> comparator = Comparator.comparing(Row::getKey).thenComparing(Row::getValue);

    /**
     * One line in storage with information about one record.
     *
     * @param key uniq identifier
     * @param value data
     */
    public Row(@NotNull final ByteBuffer key, final Value value) {
        this.key = key;
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public static Row of(@NotNull final ByteBuffer key, final ByteBuffer value) {
        return new Row(key, Value.of(value));
    }

    public static Row ofTombstone(@NotNull final ByteBuffer key) {
        return new Row(key, Value.ofTombstone());
    }

    public int getSize() {
        return Integer.BYTES + key.remaining() + value.getSize();
    }
}
