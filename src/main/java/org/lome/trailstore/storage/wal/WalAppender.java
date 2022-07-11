package org.lome.trailstore.storage.wal;

import lombok.SneakyThrows;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;

import java.io.Closeable;
import java.io.IOException;

public interface WalAppender extends Closeable, AutoCloseable{

    public void append(Event event) throws EventAppendException;

}
