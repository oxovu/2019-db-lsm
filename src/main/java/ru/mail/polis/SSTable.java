package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTable implements MyTable {

    private int rowsNum;
    private IntBuffer offsets;
    private ByteBuffer rows;

    public SSTable(FileChannel fileChannel, StandardOpenOption read) throws IOException {
        ByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                fileChannel.size()).order(ByteOrder.BIG_ENDIAN);
        this.rowsNum = byteBuffer.getInt(byteBuffer.limit() - Integer.BYTES);
        this.offsets = byteBuffer.duplicate().position(byteBuffer.limit() - Integer.BYTES * (rowsNum + 1))
                .limit(byteBuffer.limit() - Integer.BYTES).slice().asIntBuffer();
        this.rows = byteBuffer.duplicate().position(0).limit(byteBuffer.limit() - Integer.BYTES * (rowsNum + 1))
                .slice().asReadOnlyBuffer();
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) {
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

    private int getPosition(@NotNull ByteBuffer key) {
        int left = 0;
        int right = rowsNum - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = keyAt(mid).compareTo(key);
            if (cmp < 0) left = mid + 1;
            else if (cmp > 0) right = mid - 1;
            else return mid;
        }
        return left;
    }

    private ByteBuffer keyAt(int position) {
        if (position < 0 || position > rowsNum) return null;

        final int offset = offsets.get(position);
        final int keySize = rows.getInt(offset);
        return rows.duplicate().position(offset + Integer.BYTES).limit(offset + Integer.BYTES + keySize)
                .slice().asReadOnlyBuffer();
    }

    private Row getRow(int position) {
        if (position < 0 || position > rowsNum) return null;

        int offset = offsets.get(position);
        int keySize = rows.getInt(offset);
        ByteBuffer key = rows.duplicate().position(offset).limit(offset + keySize).slice().asReadOnlyBuffer();
        offset += keySize;
        long timestamp = rows.position(offset).getLong();
        offset += Long.BYTES;
        if (timestamp < 0) {
            return Row.ofData(key, null, -1 * timestamp);
        }
        int valSize = rows.get(offset);
        offset += Integer.BYTES;
        ByteBuffer value = rows.duplicate().position(offset).limit(offset + valSize).slice().asReadOnlyBuffer();
        return Row.ofData(key, value, timestamp);
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        throw new UnsupportedOperationException();
    }
}
