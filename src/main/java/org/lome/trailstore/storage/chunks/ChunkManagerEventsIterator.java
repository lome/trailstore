package org.lome.trailstore.storage.chunks;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;

@Slf4j
public class ChunkManagerEventsIterator implements Iterator<EventAccessor> {

    final ChunkManager chunkManager;
    final BloomFilter<Long> bloom;
    transient long currentChunkFirst = 0L;
    transient ChunkReader currentReader;
    transient Iterator<EventAccessor> currentIterator;

    transient EventAccessor next;

    protected ChunkManagerEventsIterator(final ChunkManager manager){
        this.chunkManager = manager;
        this.bloom = BloomFilter.create(
                Funnels.longFunnel(),
                10000000L,
                0.001
        );
        nextReader();
    }

    @Override
    public boolean hasNext() {
        while(_hasNext()){
            this.next = currentIterator.next();
            long nextId = this.next.getId();
            if (!bloom.mightContain(nextId)){
                bloom.put(nextId);
                return true;
            }
            log.warn("Bloom found dup! {}",this.next.getId());
        }
        return false;
    }

    boolean _hasNext(){
        if (currentReader.isClosed()){
            if (nextReader()) return _hasNext();
            else return false;
        }else{
            return currentIterator.hasNext();
        }
    }

    @Override
    public EventAccessor next() {
        return next;
    }

    private boolean nextReader() {
        // Look through stored chunks
        ChunkReader stored = chunkManager.storedChunksSet()
                .stream().filter(c -> c.getInfo().getFirst() > this.currentChunkFirst)
                .filter(c -> c.isValid())
                .findFirst()
                .map(c -> c.getReader())
                .orElse(null);
        if (stored != null){
            try {
                log.info("Moving reader to {}",stored.info().getFirst());
                currentChunkFirst = stored.info().getFirst();
                currentIterator = stored.eventIterator();
                currentReader = stored;
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

        return false;
    }


}
