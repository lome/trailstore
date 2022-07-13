package org.lome.trailstore.storage.chunks;

import lombok.SneakyThrows;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ChunkFileReader
        extends BaseChunk
        implements Closeable, AutoCloseable, ChunkReader {


    final AtomicBoolean closed = new AtomicBoolean(false);

    public ChunkFileReader(File in) throws IOException {
        super(in);
    }

    protected long getId(int index){
        return idVector.get(index);
    }

    protected byte[] getKey(int index){
        return keyVector.get(index);
    }

    protected byte[] getMetadata(int index){
        return metadataVector.get(index);
    }

    protected byte[] getData(int index){
        return dataVector.get(index);
    }

    public Stream<EventAccessor> eventStream() throws ChunkClosedException {
        if (closed.get()){
            throw new ChunkClosedException();
        }
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
        final ChunkFileReader chunkReader = this;
        return new Iterator<EventAccessor>() {

            int currentBatchSize = -1;
            int currentBatchIndex = -1;

            @Override
            public boolean hasNext() {
                if (currentBatchSize == currentBatchIndex){
                    try {
                        if (!reader.loadNextBatch()){
                            chunkReader.close();
                            return false;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    currentBatchSize = schemaRoot.getRowCount();
                    currentBatchIndex = 0;
                }
                //Empty Batches ?
                return currentBatchIndex < currentBatchSize ? true : hasNext();
            }

            @Override
            public EventAccessor next() {
                final int aidx = currentBatchIndex;
                EventAccessor ev = new EventAccessor() {
                    @Override
                    public long getId() {
                        return chunkReader.getId(aidx);
                    }

                    @Override
                    public byte[] getKey() {
                        return chunkReader.getKey(aidx);
                    }

                    @Override
                    public byte[] getMetadata() {
                        return chunkReader.getMetadata(aidx);
                    }

                    @Override
                    public byte[] getData() {
                        return chunkReader.getData(aidx);
                    }
                };
                currentBatchIndex++;
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
        return closed.get();
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
        super.close();
    }

    @Override
    public ChunkInfo info() throws ChunkClosedException {
        {
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
}
