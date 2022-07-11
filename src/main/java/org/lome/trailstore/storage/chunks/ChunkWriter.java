package org.lome.trailstore.storage.chunks;

import org.lome.trailstore.model.Event;

import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Stream;

public interface ChunkWriter extends Closeable,AutoCloseable {

    void append(Event event) throws ChunkClosedException, IOException;
    void append(Stream<Event> events) throws ChunkClosedException, IOException;

}
