package org.lome.trailstore.storage.wal;

import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.utils.Sequencer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.IntStream;

@Slf4j
public class WalTest {

    public static void main(String[] args) throws IOException {

        final Path path = Path.of("foo.wal");

        final int events = 1000;

        final boolean write = true;
        final boolean read = true;

        WalFileAppender appender = new WalFileAppender(path);
        WalFileScanner scanner = new WalFileScanner(path);
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
                    int rows = 0;
                    while(scanner.hasNext()){
                        scanner.next();
                        rows++;
                    }
                    System.out.println(rows);
                });
        appender.close();
        scanner.close();
    }

}
