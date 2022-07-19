package org.lome.trailstore.storage.chunks;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.h2.mvstore.Chunk;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.utils.Sequencer;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


//Needs: --add-opens=java.base/java.nio=ALL-UNNAMED
@Slf4j
public class MemoryChunk
        extends BaseChunk
        implements ChunkWriter, AutoCloseable, Closeable {

    final AtomicInteger readers;
    @Getter
    final AtomicLong first;
    @Getter
    final AtomicLong last;

    public MemoryChunk() throws IOException {
        this.readers = new AtomicInteger(0);
        this.first = new AtomicLong(-1L);
        this.last = new AtomicLong(-1L);
    }

    public void append(Event event){
        int index = size();
        idVector.setSafe(index,event.getId());
        keyVector.setSafe(index,event.getKey());
        metadataVector.setSafe(index, event.getMetadata());
        dataVector.setSafe(index, event.getData());
        schemaRoot.setRowCount(index+1);
        this.first.updateAndGet(v -> v <= 0 ? event.getId() : v);
        this.last.updateAndGet(v -> Math.max(v,event.getId()));
        //log.info("{}/{}",this.first.get(),this.last.get());
    }

    public void append(Stream<Event> events){
        events.forEach(this::append);
    }

    public int size(){
        return schemaRoot.getRowCount();
    }

    public void store(File output) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        FileOutputStream fileOutputStream = new FileOutputStream(output);
        ArrowStreamWriter writer = new ArrowStreamWriter(schemaRoot, dictProvider, fileOutputStream.getChannel());
        writer.start();
        writer.writeBatch();
        writer.close();
        _close();
    }

    public ChunkReader reader(){
        readers.incrementAndGet();
        return new ChunkReader() {

            boolean closed = false;
            @Override
            public Stream<EventAccessor> eventStream() throws ChunkClosedException {
                Iterable<EventAccessor> eventSupplier = new Iterable<EventAccessor>() {

                    @Override
                    @SneakyThrows
                    public Iterator<EventAccessor> iterator() {
                        return eventIterator();
                    }
                };
                return StreamSupport.stream(eventSupplier.spliterator(), false);
            }

            @Override
            public Iterator<EventAccessor> eventIterator() throws ChunkClosedException {
                return new Iterator<EventAccessor>() {

                    int currentIndex = 0;
                    @Override
                    public boolean hasNext() {
                        return currentIndex < schemaRoot.getRowCount();
                    }

                    @Override
                    public EventAccessor next() {
                        final int aidx = currentIndex;
                        EventAccessor ev = new EventAccessor() {
                            @Override
                            public long getId() {
                                try {
                                    return idVector.get(aidx);
                                }catch(IllegalStateException e){
                                    log.error("Illegal EX: {}",readers.get());
                                    throw e;
                                }
                            }

                            @Override
                            public byte[] getKey() {
                                return keyVector.get(aidx);
                            }

                            @Override
                            public byte[] getMetadata() {
                                return metadataVector.get(aidx);
                            }

                            @Override
                            public byte[] getData() {
                                return dataVector.get(aidx);
                            }
                        };
                        currentIndex++;
                        if (!hasNext()) {
                            try {
                                close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return ev;
                    }
                };
            }

            @Override
            public Stream<Long> idStream() throws ChunkClosedException {
                if (isClosed()) throw new ChunkClosedException();
                return eventStream().map(e -> e.getId());
            }

            @Override
            public boolean isClosed() {
                return closed;
            }

            @Override
            public ChunkInfo info() throws ChunkClosedException {
                if (isClosed()) throw new ChunkClosedException();
                ChunkInfo info = ChunkInfo.builder()
                        .elements(0)
                        .first(Long.MAX_VALUE)
                        .last(Long.MIN_VALUE)
                        .build();
                idStream()
                        .forEach(id -> {
                            info.elements++;
                            info.first = Math.min(info.first,id);
                            info.last = Math.max(info.last,id);
                        });
                return info;
            }

            @Override
            public void close() throws IOException {
                if (!closed){
                    closed = true;
                    readers.decrementAndGet();
                    _close();
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        //No way
    }

    void _close() throws IOException {
        if (readers.get() > 0){
            log.warn("Won't close Memory Chunk, {} readers still alive",readers.get());
        }else{
            log.info("Closing memory chunk");
            super.close();
        }
    }
}
