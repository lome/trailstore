package org.lome.trailstore.storage.chunks;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class ChunkManagerEventsIterator implements Iterator<EventAccessor> {

    final ChunkManager chunkManager;
    final Roaring64Bitmap bloom;
    transient long currentChunkFirst = 0L;
    transient ChunkReader currentReader;
    transient Iterator<EventAccessor> currentIterator;

    transient EventAccessor next;

    protected ChunkManagerEventsIterator(final ChunkManager manager){
        this.chunkManager = manager;
        this.bloom = new Roaring64Bitmap();
        nextReader();
    }

    @Override
    public boolean hasNext() {
        while(_hasNext()){
            this.next = currentIterator.next();
            long nextId = this.next.getId();
            if (!bloom.contains(nextId)){
                bloom.add(nextId);
                return true;
            }
            //log.warn("Bloom found dup! {}",this.next.getId());
        }
        return false;
    }

    boolean _hasNext(){
        if (currentReader == null || currentIterator == null) return false;
        if (currentReader.isClosed() || !currentIterator.hasNext()){
            if (nextReader()) return _hasNext();
            else return false;
        }else{
            return true;
        }
    }

    @Override
    public EventAccessor next() {
        return next;
    }

    private boolean nextReader() {
        // Look through stored chunks
        ChunkManager.Chunker stored = chunkManager.storedChunksSet()
                .stream().filter(c -> c.getInfo().getFirst() > this.currentChunkFirst)
                .filter(c -> c.isValid())
                .findFirst()
                .orElse(null);
        if (stored != null){
            try {
                log.info("Moving reader to [Stored Chunk] {}",stored.getInfo().getFirst());
                currentChunkFirst = stored.getInfo().getFirst();
                ChunkReader reader = stored.reader();
                currentIterator = reader.eventIterator();
                currentReader = reader;
                return true;
            } catch (ChunkClosedException e) {
                throw new RuntimeException(e);
            }
        }

        // Check storing queue
        MemoryChunk storingChunk = chunkManager.chunkStoreQueue.stream()
                .sorted((mc1,mc2) -> {
                    try {
                        long mf1 = mc1.getFirst().get();
                        long mf2 = mc2.getFirst().get();
                        return Long.compare(mf1, mf2);
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }).filter(c -> {
                    return c.getFirst().get() > this.currentChunkFirst;
                }).findFirst().orElse(null);
        if (storingChunk != null){
            try {
                log.info("Moving reader [Storing Chunk] to {}",storingChunk.getFirst().get());
                ChunkReader reader = storingChunk.reader();
                currentChunkFirst = reader.info().getFirst();
                currentIterator = reader.eventIterator();
                currentReader = reader;
                return true;
            } catch (ChunkClosedException e) {
                throw new RuntimeException(e);
            }
        }

        MemoryChunk live = chunkManager.currentMemoryChunk;
        if (live != null && live.getFirst().get() > this.currentChunkFirst){
            try {
                log.info("Moving reader [LIVE] to {}",live.getFirst().get());
                ChunkReader reader = live.reader();
                currentChunkFirst = reader.info().getFirst();
                currentIterator = reader.eventIterator();
                currentReader = reader;
                return true;
            } catch (ChunkClosedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("No more readers available");

        return false;
    }


}
