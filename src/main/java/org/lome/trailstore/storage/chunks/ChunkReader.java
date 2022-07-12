package org.lome.trailstore.storage.chunks;

import org.lome.trailstore.model.Event;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Closeable;
import java.util.stream.Stream;

public interface ChunkReader extends Closeable,AutoCloseable {

    Stream<EventAccessor> eventStream() throws ChunkClosedException;

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

    default Stream<Event> readEvents() throws ChunkClosedException {
        return eventStream()
                .map(ea -> new Event(ea.getId(),ea.getKey(),ea.getMetadata(),ea.getData()));
    }

    default ChunkInfo info() throws ChunkClosedException {
        ChunkInfo info = ChunkInfo.builder()
                .elements(0)
                .first(Long.MAX_VALUE)
                .last(Long.MIN_VALUE)
                .build();
        eventStream()
                .map(e -> e.getId())
                .forEach(id -> {
                    info.elements++;
                    info.first = Math.min(info.first,id);
                    info.last = Math.max(info.last,id);
                });
        return info;
    }


}
