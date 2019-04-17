package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MyDAO implements DAO {
  private final NavigableMap<ByteBuffer, Record> database = new TreeMap<>();

  @NotNull
  @Override
  public Iterator<Record> iterator(@NotNull ByteBuffer from){
    return database.tailMap(from).values().iterator();
  }

  @Override
  public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value){
    database.put(key, Record.of(key, value));
  }

  @Override
  public void remove(@NotNull ByteBuffer key){
    database.remove(key);
  }

  @Override
  public void close(){

  }
}
