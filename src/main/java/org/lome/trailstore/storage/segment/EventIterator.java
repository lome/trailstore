package org.lome.trailstore.storage.segment;

import lombok.Getter;
import org.apache.arrow.vector.complex.reader.FieldReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class EventIterator implements Iterator<EventAccessor>,
    Comparable<EventIterator>, Closeable {

    int index;
    FieldReader keyReader;
    FieldReader idReader;
    FieldReader metadataReader;
    FieldReader dataReader;
    @Getter
    ArrowSegment segment;

    public EventIterator(ArrowSegment segment){
        reload(segment);
        this.index = 0;
    }

    long first(){
        return segment.first();
    }

    long last(){
        return segment.last();
    }

    void reload(ArrowSegment segment){
        this.segment = segment;
        this.keyReader = segment.keyVector().getReader();
        this.idReader = segment.idVector().getReader();
        this.metadataReader = segment.metadataVector().getReader();
        this.dataReader = segment.dataVector().getReader();
    }

    public void swapSource(ArrowSegment newSource){
        reload(newSource);
    }

    void placeReaders(){
        keyReader.setPosition(index);
        idReader.setPosition(index);
        metadataReader.setPosition(index);
        dataReader.setPosition(index);
    }

    @Override
    public boolean hasNext() {
        if (segment.rows() > index){
            placeReaders();
            index++;
            return true;
        }
        try {
            close();
        } catch (IOException e) {
            //Nope
        }
        return false;
    }

    @Override
    public EventAccessor next() {
        return new EventAccessor() {
            @Override
            public long getId() {
                try{
                    return idReader.readLong();
                }catch(NullPointerException e){
                    System.err.println("NPE! "+segment.getClass().toString()+" ["+index+"]");
                    throw e;
                }
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

    @Override
    public int compareTo(EventIterator o) {
        return Long.compare(first(),o.first());
    }

    @Override
    public void close() throws IOException {
        //Override me
    }
}
