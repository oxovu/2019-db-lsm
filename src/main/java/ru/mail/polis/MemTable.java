package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements MyTable {

    private final NavigableMap<ByteBuffer, Row> memtable = new TreeMap<>();
    private long size;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return memtable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, final ByteBuffer value) {
        final Row newRow = Row.of(key, value);
        memtable.put(key, newRow);
        size += 2 * Integer.BYTES + Long.BYTES + key.remaining() + value.remaining();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Row tombstone = Row.ofTombstone(key);
        memtable.put(key, tombstone);
        size += Integer.BYTES + Long.BYTES + key.remaining();
    }

    public long getSize() {
        return size;
    }

    /**
     * @param fileChannel
     * @throws IOException
     */
    public void flush(final FileChannel fileChannel) throws IOException {
        final ByteBuffer offsets = ByteBuffer.allocate(memtable.size() * Integer.BYTES);
        final ByteBuffer rows = ByteBuffer.allocate(Integer.BYTES);
        int offset = 0;
        for (final Row row : memtable.values()) {
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
        rows.putInt(memtable.size());
        rows.rewind();
        fileChannel.write(rows);
        memtable.clear();
        size = 0;
    }
}
