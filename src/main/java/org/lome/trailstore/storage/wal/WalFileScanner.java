package org.lome.trailstore.storage.wal;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventIntegrityException;
import org.lome.trailstore.exceptions.EventReadException;
import org.lome.trailstore.model.Event;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

@Slf4j
public class WalFileScanner implements WalScanner {

    final static int BUFFER_SIZE = 10*1024*1024; //10MB
    final Path filePath;
    final FileChannel channel;
    ByteBuffer currentBuffer = ByteBuffer.allocate(0);


    public WalFileScanner(Path filePath) throws IOException {
        if (!Files.exists(filePath)) throw new RuntimeException("File "+filePath+" does not exists");
        RandomAccessFile reader = new RandomAccessFile(filePath.toFile(), "r");
        this.filePath = filePath;
        this.channel = reader.getChannel();
        this.channel.position(0);
        updateBuffer();
    }

    @SneakyThrows
    public void reset() throws EventReadException{
        try {
            this.channel.position(0);
            this.currentBuffer = ByteBuffer.allocate(0);
            updateBuffer();
        }catch(IOException e){
            throw new EventReadException(e);
        }
    }

    @Override
    public boolean hasNext() throws EventReadException{
        try {
            return this.channel.position() < this.channel.size()
                    || this.currentBuffer.remaining() > Integer.BYTES;
        }catch(IOException e){
            throw new EventReadException(e);
        }
    }

    @Override
    public Event next() throws EventReadException, EventIntegrityException {
        if (this.currentBuffer.remaining() < Integer.BYTES) updateBuffer();
        int size = this.currentBuffer.getInt();
        if (this.currentBuffer.remaining() < size) updateBuffer();
        Event event = Event.fromBuffer(this.currentBuffer);
        return event;
    }

    void updateBuffer() throws EventReadException {
        try {
            //log.trace("Updating Buffer {}/{} - {}",
            //        this.channel.position(),
            //        this.channel.size(),
            //        this.currentBuffer.remaining());
            long start = this.channel.position() - this.currentBuffer.remaining();
            this.channel.position(start);
            long readLen = Math.min(BUFFER_SIZE, channel.size() - start);
            this.currentBuffer = this.channel.map(FileChannel.MapMode.READ_ONLY,
                    start,
                    readLen);
            this.channel.position(start + readLen);
            //log.trace("Updated Buffer[{}] {}/{}",
            //        readLen,
            //        this.channel.position(),
            //        this.channel.size());
        }catch(IOException e){
            throw new EventReadException(e);
        }
    }

    @Override
    @SneakyThrows
    public void close() {
        this.channel.close();
        this.currentBuffer.clear();
    }
}
