package segment;

import org.junit.jupiter.api.Test;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.chunks.ChunkClosedException;
import org.lome.trailstore.storage.segment.SegmentManager;
import org.lome.trailstore.utils.Sequencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PerfSegmentTest {

    final static Logger log = LoggerFactory.getLogger(PerfSegmentTest.class);

    @Test
    public void perfWrite() throws IOException, ChunkClosedException {
        clear(Path.of("segments"));
        clear(Path.of("wals"));

        SegmentManager manager = new SegmentManager(Path.of("segments"),Path.of("wals"));
        Set<Long> idStack = new HashSet<>();
        long start = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger();
        IntStream.range(0, 100001)
                .forEach(i -> {
                    try {
                        long id = Sequencer.SHARED.tick();
                        idStack.add(id);
                        manager.append(new Event(id,
                                "foo".getBytes(StandardCharsets.UTF_8),
                                "bar".getBytes(StandardCharsets.UTF_8),
                                "baz".getBytes(StandardCharsets.UTF_8)));
                        counter.incrementAndGet();
                    } catch (EventAppendException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
        double elapsed = (System.currentTimeMillis()-start)/1000.0;
        log.info("Write throughput: {} ev/sec",(counter.get()/elapsed));
        start = System.currentTimeMillis();
        counter.set(0);
        manager.iterator()
                .forEachRemaining(ea -> {
                    assertEquals(true,idStack.remove(ea.getId()));
                    counter.incrementAndGet();
                });
        manager.close();
        elapsed = (System.currentTimeMillis()-start)/1000.0;
        log.info("Read throughput: {} ev/sec",(counter.get()/elapsed));

        log.info("Remaining: {}",idStack);
        assertEquals(0,idStack.size());

        clear(Path.of("segments"));
        clear(Path.of("wals"));
    }

    private void clear(Path rootPath){
        try (Stream<Path> walk = Files.walk(rootPath)) {
            walk.sorted(Comparator.reverseOrder())
                    .filter(p -> !p.equals(rootPath))
                    .forEach(this::clear);
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
        try {
            Files.delete(rootPath);
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }

}
