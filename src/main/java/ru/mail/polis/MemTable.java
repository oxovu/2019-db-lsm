package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements MyTable {

    private NavigableMap<ByteBuffer, Row> memoryTable = new TreeMap<>();
    private long size;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return memoryTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, final ByteBuffer value) {
        final Row newRow = Row.of(key, value);
        final Row prevRow = memoryTable.put(key, newRow);
        if (prevRow == null) {
            size += newRow.getSize();
        } else {
            size += newRow.getValue().getSize() - prevRow.getValue().getSize();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Row newRow = Row.ofTombstone(key);
        final Row prevRow = memoryTable.put(key, newRow);
        if (prevRow == null) {
            size += newRow.getSize();
        } else {
            size += newRow.getValue().getSize() - prevRow.getValue().getSize();
        }
    }

    public long getSize() {
        return size;
    }

    /**
     * Restores data from memory to storage.
     *
     * @param path where to store
     * @throws IOException if it is impossible to store
     */
    public void flush(final Path path) throws IOException {
        SSTable.writeToFile(path, memoryTable.values().iterator());
        memoryTable = new TreeMap<>();
        size = 0;
    }
}
