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
                        long mf1 = mc1.info().getFirst();
                        long mf2 = mc2.info().getFirst();
                        return Long.compare(mf1, mf2);
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }).filter(c -> {
                    try {
                        return c.info().getFirst() > this.currentChunkFirst;
                    } catch (ChunkClosedException e) {
                        throw new RuntimeException(e);
                    }
                }).findFirst().orElse(null);
        if (storingChunk != null){
            try {
                log.info("Moving reader [Storing Chunk] to {}",storingChunk.info().getFirst());
                currentChunkFirst = storingChunk.info().getFirst();
                currentIterator = storingChunk.eventIterator();
                currentReader = storingChunk;
                return true;
            } catch (ChunkClosedException e) {
                throw new RuntimeException(e);
            }
        }

        MemoryChunk live = chunkManager.currentMemoryChunk;
        try {
            if (live != null && live.info().getFirst() > this.currentChunkFirst){
                try {
                    log.info("Moving reader [LIVE] to {}",live.info().getFirst());
                    currentChunkFirst = live.info().getFirst();
                    currentIterator = live.eventIterator();
                    currentReader = live;
                    return true;
                } catch (ChunkClosedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (ChunkClosedException e) {
            throw new RuntimeException(e);
        }
        log.info("No more readers available");

        return false;
    }


}
