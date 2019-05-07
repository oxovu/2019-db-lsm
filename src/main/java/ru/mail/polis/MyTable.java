package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface MyTable {

    @NotNull
    Iterator<Row> iterator(@NotNull ByteBuffer from);

    void upsert(@NotNull ByteBuffer key, ByteBuffer value);

    void remove(@NotNull ByteBuffer key);
}
