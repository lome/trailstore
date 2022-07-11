package org.lome.trailstore.storage.wal;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.utils.Sequencer;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.IntStream;

@Slf4j
public class WalFileAppender implements Closeable, AutoCloseable {

    final Path filePath;
    final FileChannel channel;

    public WalFileAppender(Path filePath) throws IOException {
        RandomAccessFile reader = new RandomAccessFile(filePath.toFile(), "rw");
        this.filePath = filePath;
        this.channel = reader.getChannel();
        this.channel.position(this.channel.size());
    }

    public void append(Event event) throws EventAppendException {
        ByteBuffer buf = event.toBuffer();
        try {
            this.channel.write(buf);
        } catch (IOException e) {
            throw new EventAppendException(e);
        } finally {
            buf.clear();
        }
    }

    public long size() throws EventAppendException {
        try {
            return this.channel.size();
        } catch (IOException e) {
            throw new EventAppendException(e);
        }
    }

    @Override
    @SneakyThrows
    public void close(){
        this.channel.force(true);
        this.channel.close();
    }

    public static void main(String[] args) throws IOException {

        final Path path = Path.of("foo.wal");

        final int events = 1000000;

        final boolean write = true;
        final boolean read = true;

        if (write) {
            long start = System.currentTimeMillis();
            WalFileAppender appender = new WalFileAppender(path);
            IntStream.range(0, events)
                    .forEach(i -> {
                        try {
                            appender.append(new Event(Sequencer.SHARED.tick(),
                                    "foo".getBytes(StandardCharsets.UTF_8),
                                    "bar".getBytes(StandardCharsets.UTF_8),
                                    "baz".getBytes(StandardCharsets.UTF_8)));
                        } catch (EventAppendException e) {
                            throw new RuntimeException(e);
                        }
                    });
            appender.close();
            long elapsed = System.currentTimeMillis() - start;
            log.info("Append: {} event/sec [{}]", (events / ((elapsed / 1000.0))), events);
        }

        if (read) {
            long start = System.currentTimeMillis();
            WalFileScanner scanner = new WalFileScanner(path);
            int rread = 0;
            while (scanner.hasNext()) {
                rread++;
                try {
                    scanner.next();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    System.out.println("READ: "+rread);
                    System.exit(-1);
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            log.info("Scan: {} event/sec [{}]", (rread / ((elapsed / 1000.0))), rread);
        }
    }

}
