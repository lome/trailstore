package org.lome.trailstore.storage.chunks;

import org.lome.trailstore.model.Event;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Closeable;
import java.util.Iterator;
import java.util.stream.Stream;

public interface ChunkReader extends Closeable,AutoCloseable {

    Stream<EventAccessor> eventStream() throws ChunkClosedException;
    Iterator<EventAccessor> eventIterator() throws ChunkClosedException;
    Stream<Long> idStream() throws ChunkClosedException;
    boolean isClosed();

    default Stream<EventAccessor> eventStream(Roaring64Bitmap ids) throws ChunkClosedException {
        return eventStream()
                .filter(e -> ids.contains(e.getId()));
    }

    default Roaring64Bitmap eventIndex(EventFilter filter) throws ChunkClosedException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        eventStream()
                .filter(filter::filter)
                .map(ea -> ea.getId())
                .forEach(bitmap::add);
        bitmap.runOptimize();
        return bitmap;
    }

    default Stream<EventAccessor> eventStream(EventFilter filter) throws ChunkClosedException {
        return eventStream()
                .filter(filter::filter);
    }

    default Stream<Event> readEvents() throws ChunkClosedException {
        return eventStream()
                .map(ea -> new Event(ea.getId(),ea.getKey(),ea.getMetadata(),ea.getData()));
    }

    ChunkInfo info() throws ChunkClosedException;


}
