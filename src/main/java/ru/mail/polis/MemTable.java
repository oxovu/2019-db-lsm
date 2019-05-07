package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements MyTable {

    private NavigableMap<ByteBuffer, Row> memtable = new TreeMap<>();
    private long size = 0;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) {
        return memtable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, ByteBuffer value) {
        Row newRow = Row.of(key, value);
        memtable.put(key, newRow);
        size += 2 * Integer.BYTES + Long.BYTES + key.remaining() + value.remaining();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        Row tombstone = Row.ofTombstone(key);
        memtable.put(key, tombstone);
        size += Integer.BYTES + Long.BYTES + key.remaining();
    }

    public long getSize() {
        return size;
    }

    public void flush(FileChannel fileChannel) throws IOException {
        ByteBuffer offsets = ByteBuffer.allocate(memtable.size() * Integer.BYTES);
        ByteBuffer rows = ByteBuffer.allocate(Integer.BYTES);
        int offset = 0;
        for (Row row : memtable.values()) {
            offsets.putInt(offset);
            ByteBuffer rowBuffer = ByteBuffer.allocate(row.getSize());
            ByteBuffer key = row.getKey();
            ByteBuffer value = row.getValue();
            long timestamp = row.getTimestamp();
            rowBuffer.putInt(key.remaining());
            rowBuffer.put(key);
            if (!row.isTombstone()) {
                rowBuffer.putLong(timestamp);
                rowBuffer.putInt(value.remaining());
                rowBuffer.put(value);
            } else {
                rowBuffer.putLong(-1 * timestamp);
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
