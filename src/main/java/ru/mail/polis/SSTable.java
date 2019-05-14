package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SSTable implements MyTable {

    private final int rowsNum;
    private final IntBuffer offsets;
    private final ByteBuffer rows;
    private final Path path;

    /**
     * Storage of data.
     *
     * @param path where to write data
     * @throws IOException if it is impossible to store
     */
    public SSTable(@NotNull final Path path) throws IOException {
        this.path = path;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            final File file = path.toFile();
            if (file.length() == 0 || file.length() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            final ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0,
                    channel.size()).order(ByteOrder.BIG_ENDIAN);
            this.rowsNum = mapped.getInt(mapped.limit() - Integer.BYTES);
            final int position = mapped.limit() - Integer.BYTES * (this.rowsNum + 1);
            if (position < 0 || position > mapped.limit()) {
                throw new IllegalArgumentException();
            }
            this.offsets = mapped.duplicate().position(position).limit(mapped.limit() - Integer.BYTES)
                    .slice().asIntBuffer();
            this.rows = mapped.duplicate().limit(offsets.position()).slice();
        }
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            private int pos = getPosition(from);

            @Override
            public boolean hasNext() {
                return pos < rowsNum;
            }

            @Override
            public Row next() {
                return getRow(pos++);
            }
        };
    }

    private int getPosition(@NotNull final ByteBuffer key) {
        int left = 0;
        int right = rowsNum - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = keyAt(mid).compareTo(key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else return mid;
        }
        return left;
    }

    @NotNull
    private ByteBuffer keyAt(final int position) {
        if (position < 0 || position > rowsNum) {
            throw new IllegalArgumentException();
        }

        final int offset = offsets.get(position);
        final int keySize = rows.getInt(offset);
        return rows.duplicate().position(offset + Integer.BYTES).limit(offset + Integer.BYTES + keySize)
                .slice();
    }

    private Row getRow(final int position) {
        if (position < 0 || position > rowsNum)  {
            throw new IllegalArgumentException();
        }

        int offset = offsets.get(position);
        final int keySize = rows.getInt(offset);
        final ByteBuffer key = rows.duplicate().position(offset).limit(offset + Integer.BYTES + keySize).slice();
        offset += Integer.BYTES + keySize;
        final long timestamp = rows.position(offset).getLong();
        offset += Long.BYTES;
        if (timestamp < 0) {
            return new Row(key, new Value(Value.EMPTY, timestamp * -1, true));
        }
        final int valSize = rows.getInt(offset);
        offset += Integer.BYTES;
        final ByteBuffer data = rows.duplicate().position(offset).limit(offset + valSize).slice();
        return new Row(key, new Value(data, timestamp, false));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, final ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes to file the rows with keys and values, offsets and rows number.
     *
     * @param path where to write
     * @param iterator just iterator
     * @throws IOException if occurs
     */
    public static void writeToFile(@NotNull final Path path, @NotNull final Iterator<Row> iterator) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            ByteBuffer rows;
            Row row;
            ByteBuffer key;
            Value value;
            int offset = 0;
            while (iterator.hasNext()) {
                row = iterator.next();
                offsets.add(offset);
                rows = ByteBuffer.allocate(row.getSize());
                key = row.getKey();
                value = row.getValue();
                rows.putInt(key.remaining()).put(key);
                if (value.isTombstone()) {
                    rows.putLong(value.getTimestamp() * -1);
                } else {
                    rows.putLong(value.getTimestamp()).putInt(value.getSize()).put(value.getData());
                }
                rows.rewind();
                channel.write(rows);
                offset += row.getSize();
            }
            final ByteBuffer offsetsBuff =  ByteBuffer.allocate(Integer.BYTES * offsets.size());
            for (final Integer v : offsets) {
                offsetsBuff.putInt(v);
            }
            offsetsBuff.rewind();
            channel.write(offsetsBuff);
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES).putInt(offsets.size()).rewind();
            channel.write(size);
        }
    }

    public Path getPath() {
        return path;
    }
}
