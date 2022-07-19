package org.lome.trailstore.storage.segment;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

public class ArrowFileSegment implements ArrowSegment {

    final VectorSchemaRoot vectorSchema;
    final FileInputStream fileInputStream;
    final ArrowFileReader reader;

    public ArrowFileSegment(File in) throws IOException {
        fileInputStream = new FileInputStream(in);
        reader = new ArrowFileReader(fileInputStream.getChannel(), ROOT_ALLOCATOR);
        reader.initialize();
        reader.loadNextBatch();
        this.vectorSchema = reader.getVectorSchemaRoot();
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

    public EventIterator iterator(){
        return new EventIterator(this);
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

    @Override
    public void close() throws IOException {
        this.vectorSchema.clear();
        this.vectorSchema.close();
        this.fileInputStream.close();
    }
}
