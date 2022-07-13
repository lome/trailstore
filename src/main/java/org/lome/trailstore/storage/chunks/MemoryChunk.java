package org.lome.trailstore.storage.chunks;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.utils.Sequencer;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


//Needs: --add-opens=java.base/java.nio=ALL-UNNAMED
@Slf4j
public class MemoryChunk
        extends BaseChunk
        implements ChunkReader, ChunkWriter, AutoCloseable, Closeable {

    public MemoryChunk() throws IOException {
    }

    public void append(Event event){
        int index = size();
        idVector.setSafe(index,event.getId());
        keyVector.setSafe(index,event.getKey());
        metadataVector.setSafe(index, event.getMetadata());
        dataVector.setSafe(index, event.getData());
        schemaRoot.setRowCount(index+1);
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
    }

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
                        }catch(IndexOutOfBoundsException e){
                            log.error("Index Out  of Bounds! currentIndex={}, aidx={}, size={}",
                                    currentIndex,aidx,size());
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
                return ev;
            }
        };
    }

    @Override
    public Stream<Long> idStream() throws ChunkClosedException {
        return eventStream().map(e -> e.getId());
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    public static void main(String[] args) throws IOException, ChunkClosedException {
        MemoryChunk chunk = new MemoryChunk();
        IntStream.range(0, 100)
                .forEach(i -> {
                    try {
                        chunk.append(new Event(Sequencer.SHARED.tick(),
                                "foo".getBytes(StandardCharsets.UTF_8),
                                "bar".getBytes(StandardCharsets.UTF_8),
                                "baz".getBytes(StandardCharsets.UTF_8)));
                    } catch (EventAppendException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
        System.out.println("CHUNK SIZE: "+chunk.size());
        int evSize = chunk.eventStream().collect(Collectors.toList()).size();
        System.out.println("EVENTS SIZE: "+evSize);
        chunk.eventStream().forEach(e -> System.out.println(e.getId()));
    }

    @Override
    public ChunkInfo info() throws ChunkClosedException {
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
}
