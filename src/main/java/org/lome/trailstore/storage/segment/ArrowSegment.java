package org.lome.trailstore.storage.segment;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.impl.VarBinaryReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lome.trailstore.model.Event;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class ArrowSegment implements Closeable {

    final static RootAllocator ROOT_ALLOCATOR = new RootAllocator();

    final VectorSchemaRoot vectorSchema;

    public ArrowSegment(VectorSchemaRoot vectorSchema){
        this.vectorSchema = vectorSchema;
    }

    public ArrowSegment(){
        this(VectorSchemaRoot.create(arrowSchema(), ROOT_ALLOCATOR));
    }

    public int rows(){
        return vectorSchema.getRowCount();
    }

    public long first(){
        if (rows() < 1) return -1;
        return idVector().get(1);
    }

    public long last(){
        if (rows() < 1) return -1;
        return idVector().get(rows()-1);
    }

    public synchronized void append(Event event){
        int index = vectorSchema.getRowCount();
        // Do Not call allocate new! this seems to cause vector inconsistency
        //vectorSchema.allocateNew();
        idVector().setSafe(index,event.getId());
        keyVector().setSafe(index,event.getKey());
        metadataVector().setSafe(index, event.getMetadata());
        dataVector().setSafe(index, event.getData());
        vectorSchema.setRowCount(index+1);
    }

    public Iterator<EventAccessor> iterator(){
        final FieldReader keyReader = keyVector().getReader();
        final FieldReader idReader = idVector().getReader();
        final FieldReader metadataReader = metadataVector().getReader();
        final FieldReader dataReader = dataVector().getReader();
        return new Iterator<EventAccessor>() {

            int index = 0;
            void placeReaders(){
                keyReader.setPosition(index);
                idReader.setPosition(index);
                metadataReader.setPosition(index);
                dataReader.setPosition(index);
            }

            @Override
            public boolean hasNext() {
                return index < rows();
            }

            @Override
            public synchronized EventAccessor next() {
                index++;
                placeReaders();
                return new EventAccessor() {
                    @Override
                    public long getId() {
                        return idReader.readLong();
                    }

                    @Override
                    public byte[] getKey() {
                        return keyReader.readByteArray();
                    }

                    @Override
                    public byte[] getMetadata() {
                        return metadataReader.readByteArray();
                    }

                    @Override
                    public byte[] getData() {
                        return dataReader.readByteArray();
                    }
                };
            }
        };
    }

    public BigIntVector idVector(){
        return (BigIntVector)vectorSchema.getVector("id");
    }

    public VarBinaryVector keyVector(){
        return (VarBinaryVector)vectorSchema.getVector("key");
    }

    public VarBinaryVector metadataVector(){
        return (VarBinaryVector)vectorSchema.getVector("metadata");
    }

    public VarBinaryVector dataVector(){
        return (VarBinaryVector)vectorSchema.getVector("data");
    }

    static Schema arrowSchema(){
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

    public void store(File output) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        FileOutputStream fileOutputStream = new FileOutputStream(output);
        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchema, dictProvider, fileOutputStream.getChannel());
        writer.start();
        writer.writeBatch();
        writer.close();
    }


    @Override
    public void close() throws IOException {
        this.vectorSchema.clear();
        this.vectorSchema.close();
    }
}
