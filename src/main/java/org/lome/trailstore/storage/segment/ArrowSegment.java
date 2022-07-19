package org.lome.trailstore.storage.segment;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

public interface ArrowSegment extends Closeable, Comparable<ArrowSegment> {

    final static RootAllocator ROOT_ALLOCATOR = new RootAllocator();
    public int rows();
    public long first();
    public long last();
    public EventIterator iterator();
    public BigIntVector idVector();
    public VarBinaryVector keyVector();
    public VarBinaryVector metadataVector();
    public VarBinaryVector dataVector();

    @Override
    default int compareTo(ArrowSegment o) {
        return Long.compare(this.first(),o.first());
    }
}
