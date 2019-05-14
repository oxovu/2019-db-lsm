package ru.mail.polis;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.FileVisitOption;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

public class MyPersDAO implements DAO {

    private static final String SUFFIX = ".db";
    private static final String TMP_SUFFIX = ".txt";

    private final File dir;
    private final long maxSize;
    private final MemTable memTable;
    private final List<SSTable> storage;

    private static final Logger log = LoggerFactory.getLogger(MyPersDAO.class);

    /**
     * Persistence DAO.
     *
     * @param dir     directory with files
     * @param maxSize maximum size of memory table
     */
    public MyPersDAO(@NotNull final File dir, @NotNull final long maxSize) throws IOException {
        this.dir = dir;
        this.maxSize = maxSize;
        memTable = new MemTable();
        storage = new ArrayList<>();
        readStorage();
    }

    private void readStorage() throws IOException {
        Files.walkFileTree(dir.toPath(), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                if (path.toString().endsWith(SUFFIX)) {
                    try {
                        storage.add(new SSTable(path));
                    } catch (IllegalArgumentException iae) {
                        log.error("Cannot create SSTable from " + path.getFileName() + ": " + iae.getMessage());
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Row> rowIterator = rowIterator(from);
        return Iterators.transform(rowIterator, i -> Record.of(i.getKey(), i.getValue().getData()));
    }

    private Iterator<Row> rowIterator(@NotNull final ByteBuffer from) {
        final ArrayList<Iterator<Row>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable s : storage) {
            iterators.add(s.iterator(from));
        }
        final Iterator<Row> merged = Iterators.mergeSorted(iterators, Row.comparator);
        final Iterator<Row> collapsed = Iters.collapseEquals(merged, Row::getKey);
        return Iterators.filter(collapsed, i -> !i.getValue().isTombstone());
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
        if (memTable.getSize() > 0) flushTable();
    }

    private void flushTable() throws IOException {
        final String time = String.valueOf(System.currentTimeMillis());
        final String tmpName = time + TMP_SUFFIX;
        final String Name = time + SUFFIX;
        final String path = dir.getAbsolutePath();
        memTable.flush(Path.of(path, tmpName));
        Files.move(Path.of(path, tmpName), Path.of(path, Name), StandardCopyOption.ATOMIC_MOVE);
        storage.add(new SSTable(Path.of(path, Name)));
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Row> rowIter = rowIterator(Value.EMPTY);
        final String time = String.valueOf(System.currentTimeMillis());
        final String tmpName = time + TMP_SUFFIX;
        final String Name = time + SUFFIX;
        final String path = dir.getAbsolutePath();
        SSTable.writeToFile(Path.of(path, tmpName), rowIter);
        for (final SSTable s : storage) {
            Files.delete(s.getPath());
        }
        storage.clear();
        memTable.clear();
        Files.move(Path.of(path, tmpName), Path.of(path, Name), StandardCopyOption.ATOMIC_MOVE);
        storage.add(new SSTable(Path.of(path, Name)));
    }
}
