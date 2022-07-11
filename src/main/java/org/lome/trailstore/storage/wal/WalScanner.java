package org.lome.trailstore.storage.wal;

import org.lome.trailstore.model.Event;

import java.io.Closeable;
import java.util.Iterator;

public interface WalScanner extends Iterator<Event>, Closeable,AutoCloseable{
    public void reset();
}
