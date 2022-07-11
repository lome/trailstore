package org.lome.trailstore.storage.chunks;

import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lome.trailstore.model.Event;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Deprecated
public class ChunkFileWriter
        extends BaseChunk
        implements ChunkWriter, Closeable, AutoCloseable {

    final ArrowStreamWriter writer;
    final FileOutputStream fileOutputStream;
    final AtomicBoolean closed = new AtomicBoolean(false);

    public ChunkFileWriter(File out) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        fileOutputStream = new FileOutputStream(out);
        writer = new ArrowStreamWriter(schemaRoot, dictProvider, fileOutputStream.getChannel());
    }

    public static Schema arrowSchema(){
        Field idField = new Field("id", FieldType.notNullable(Types.MinorType.BIGINT.getType()),null);
        Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Binary()),null);
        Field metaField = new Field("metadata", FieldType.nullable(new ArrowType.Binary()),null);
        Field dataField = new Field("data", FieldType.notNullable(new ArrowType.Binary()),null);
        return new Schema(Arrays.asList(
                idField,
                keyField,
                metaField,
                dataField
        ),null);
    }

    public void write(Iterator<Event> events) throws IOException, ChunkClosedException {
        Stream<Event> targetStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(events, Spliterator.ORDERED),
                false);
        append(targetStream);
    }

    public void append(Event event) throws IOException, ChunkClosedException {
        append(Stream.of(event));
    }

    public void append(Stream<Event> events) throws IOException, ChunkClosedException {
        if (closed.getAndSet(true)){
            throw new ChunkClosedException();
        }

        writer.start();
        schemaRoot.allocateNew();
        AtomicInteger index = new AtomicInteger(0);
        events.forEach(event -> {
            idVector.setSafe(index.get(),event.getId());
            keyVector.setSafe(index.get(),event.getKey());
            metadataVector.setSafe(index.get(),event.getMetadata());
            dataVector.setSafe(index.get(),event.getData());
            index.incrementAndGet();
        });
        schemaRoot.setRowCount(index.get());
        writer.writeBatch();
        schemaRoot.clear();
        writer.close();
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    @Override
    public void close() throws IOException {
        fileOutputStream.close();
        rootAllocator.close();
    }
}
