package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements MyTable {

    private final NavigableMap<ByteBuffer, Row> memoryTable = new TreeMap<>();
    private long size;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return memoryTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, final ByteBuffer value) {
        final Row newRow = Row.of(key, value);
        memoryTable.put(key, newRow);
        size += Integer.BYTES + Integer.BYTES + Long.BYTES + key.remaining() + value.remaining();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Row tombstone = Row.ofTombstone(key);
        memoryTable.put(key, tombstone);
        size += Integer.BYTES + Long.BYTES + key.remaining();
    }

    public long getSize() {
        return size;
    }

    /**
     * Restores data from memory to storage
     *
     * @param fileChannel where to store
     * @throws IOException if it is impossible to store
     */
    public void flush(final FileChannel fileChannel) throws IOException {
        final ByteBuffer offsets = ByteBuffer.allocate(memoryTable.size() * Integer.BYTES);
        final ByteBuffer rows = ByteBuffer.allocate(Integer.BYTES);
        int offset = 0;
        for (final Row row : memoryTable.values()) {
            offsets.putInt(offset);
            final ByteBuffer rowBuffer = ByteBuffer.allocate(row.getSize());
            final ByteBuffer key = row.getKey();
            final ByteBuffer value = row.getValue();
            final long timestamp = row.getTimestamp();
            rowBuffer.putInt(key.remaining());
            rowBuffer.put(key);
            if (row.isTombstone()) {
                rowBuffer.putLong(-1 * timestamp);
            } else {
                rowBuffer.putLong(timestamp);
                rowBuffer.putInt(value.remaining());
                rowBuffer.put(value);
            }
            rowBuffer.rewind();
            offset += rowBuffer.remaining();
            fileChannel.write(rowBuffer);
        }
        offsets.rewind();
        fileChannel.write(offsets);
        rows.putInt(memoryTable.size());
        rows.rewind();
        fileChannel.write(rows);
        memoryTable.clear();
        size = 0;
    }
}
