package org.lome.trailstore.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lome.trailstore.exceptions.EventIntegrityException;
import org.lome.trailstore.utils.HashCrc16;
import java.nio.ByteBuffer;
import java.util.Arrays;

@AllArgsConstructor
@Getter
@ToString
public class Event {

    final static HashCrc16 crc = new HashCrc16();

    final long id;
    final byte[] key;
    final byte[] metadata;
    final byte[] data;

    public ByteBuffer toBuffer(){
        int length = bufferSize();
        ByteBuffer buf = ByteBuffer.allocateDirect(length+(Integer.BYTES));
        buf.putInt(length);
        buf.putLong(id);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(metadata.length);
        buf.put(metadata);
        buf.putInt(data.length);
        buf.put(data);
        buf.putInt(computeHash());
        buf.position(0);
        return buf;
    }

    public ByteBuffer hashBuffer(){
        int length = bufferSize();
        ByteBuffer buf = ByteBuffer.allocateDirect(length);
        buf.putLong(id);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(metadata.length);
        buf.put(metadata);
        buf.putInt(data.length);
        buf.put(data);
        buf.putInt(-1); //Check
        buf.position(0);
        return buf;
    }

    int computeHash(){
        ByteBuffer buff = hashBuffer();
        int hash = crc.computeHash(buff);
        buff.clear();
        return hash;
    }

    public int bufferSize(){
        return Long.BYTES+
                4*(Integer.BYTES)+
                key.length+metadata.length+data.length;
    }

    public static Event fromBuffer(ByteBuffer buffer) throws EventIntegrityException {
        Event ev = new Event(buffer.getLong(),
                bytes(buffer.getInt(),buffer),
                bytes(buffer.getInt(),buffer),
                bytes(buffer.getInt(),buffer));
        int crc = buffer.getInt();
        if (ev.computeHash() != crc){
            throw new EventIntegrityException("CRC mismatch! - "+crc+":"+ev.computeHash());
        }
        return ev;
    }

    static byte[] bytes(int howMany, ByteBuffer buffer){
        byte[] data = new byte[howMany];
        buffer.get(data);
        return data;
    }

    public byte[] toByteArray(){
        ByteBuffer buf = ByteBuffer.wrap(new byte[bufferSize()]);
        buf.putLong(id);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(metadata.length);
        buf.put(metadata);
        buf.putInt(data.length);
        buf.put(data);
        buf.putInt(computeHash());
        return buf.array();
    }

    public static Event fromByteArray(byte[] data) throws EventIntegrityException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Event ev = new Event(buffer.getLong(),
                bytes(buffer.getInt(),buffer),
                bytes(buffer.getInt(),buffer),
                bytes(buffer.getInt(),buffer));
        int crc = buffer.getInt();
        if (ev.computeHash() != crc){
            throw new EventIntegrityException("CRC mismatch! - "+crc+":"+ev.computeHash());
        }
        return ev;
    }



}
