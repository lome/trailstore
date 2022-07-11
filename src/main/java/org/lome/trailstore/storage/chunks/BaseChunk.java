package org.lome.trailstore.storage.chunks;

import lombok.Getter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

public class BaseChunk {

    final BigIntVector idVector;
    final VarBinaryVector keyVector;
    final VarBinaryVector metadataVector;
    final VarBinaryVector dataVector;
    final VectorSchemaRoot schemaRoot;
    final RootAllocator rootAllocator;

    final ArrowStreamReader reader;
    final FileInputStream fileInputStream;

    public BaseChunk() {
        this(new RootAllocator());
    }

    public BaseChunk(RootAllocator allocator) {
        this(allocator,VectorSchemaRoot.create(arrowSchema(), allocator));
    }

    public BaseChunk(RootAllocator allocator, VectorSchemaRoot sourceSchemaRoot) {
        rootAllocator = allocator;
        schemaRoot = sourceSchemaRoot;
        idVector = ((BigIntVector) schemaRoot.getVector("id"));
        keyVector = (VarBinaryVector) schemaRoot.getVector("key");
        metadataVector = (VarBinaryVector) schemaRoot.getVector("metadata");
        dataVector = (VarBinaryVector) schemaRoot.getVector("data");
        fileInputStream = null;
        reader = null;
    }

    public BaseChunk(File in) throws IOException {
        rootAllocator = new RootAllocator();
        fileInputStream = new FileInputStream(in);
        reader = new ArrowStreamReader(fileInputStream.getChannel(),rootAllocator);
        schemaRoot = reader.getVectorSchemaRoot();
        idVector = ((BigIntVector) schemaRoot.getVector("id"));
        keyVector = (VarBinaryVector) schemaRoot.getVector("key");
        metadataVector = (VarBinaryVector) schemaRoot.getVector("metadata");
        dataVector = (VarBinaryVector) schemaRoot.getVector("data");
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

}
