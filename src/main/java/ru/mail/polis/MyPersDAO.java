package ru.mail.polis;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class MyPersDAO implements DAO {

    private static final String SUFFIX = ".db";
    private static final String TMP_SUFFIX = ".txt";

    private final File dir;
    private final long maxSize;
    private final MemTable memTable;
    private final List<SSTable> storage;

    /**
     * Persistence DAO
     *
     * @param dir directory with files
     * @param maxSize maximum size of memtable
     */
    public MyPersDAO(@NotNull final File dir, @NotNull final long maxSize) {
        this.dir = dir;
        this.maxSize = maxSize;
        memTable = new MemTable();
        storage = new ArrayList<>();
        readStorage();
    }

    private void readStorage() {
        for (final File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().endsWith(SUFFIX)) {
                try {
                    final SSTable newSStable = new SSTable(FileChannel.open(Path.of(file.getAbsolutePath())));
                    storage.add(newSStable);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Row> rowIterator = rowIterator(from);
        return Iterators.transform(rowIterator, i -> Record.of(i.getKey(), i.getValue()));
    }

    private Iterator<Row> rowIterator(@NotNull final ByteBuffer from) {
        final ArrayList<Iterator<Row>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable s : storage) {
            iterators.add(s.iterator(from));
        }
        final Iterator<Row> merged = Iterators.mergeSorted(iterators, Row.comparator);
        final Iterator<Row> collapsed = Iters.collapseEquals(merged, Row::getKey);
        return Iterators.filter(collapsed, i -> !i.isTombstone());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.getSize() > maxSize) flushTable();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.getSize() > maxSize) flushTable();
    }

    @Override
    public void close() throws IOException {
        flushTable();
    }

    private void flushTable() throws IOException {
        final String time = String.valueOf(System.currentTimeMillis());
        final String tmpName = time + TMP_SUFFIX;
        final String path = dir.getAbsolutePath();
        try (FileChannel channel = FileChannel.open(Path.of(path, tmpName), StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            memTable.flush(channel);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final String name = time + SUFFIX;
        Files.move(Path.of(path, tmpName), Path.of(path, name), StandardCopyOption.ATOMIC_MOVE);
        storage.add(new SSTable(FileChannel.open(Path.of(path, name))));
    }
}
