package segment;

import org.junit.jupiter.api.Test;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.chunks.ChunkClosedException;
import org.lome.trailstore.storage.chunks.ChunkManager;
import org.lome.trailstore.storage.segment.SegmentManager;
import org.lome.trailstore.utils.Sequencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ConcurrentSegmentTest {

    final static Logger log = LoggerFactory.getLogger(ConcurrentSegmentTest.class);

    @Test
    public void concurrent() throws IOException, ChunkClosedException {
        clear(Path.of("segments"));
        clear(Path.of("wals"));

        SegmentManager manager = new SegmentManager(Path.of("segments"),Path.of("wals"));
        AtomicInteger added = new AtomicInteger(0);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(50);
        IntStream.range(0,50).forEach(i -> {
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    AtomicInteger counter = new AtomicInteger();
                    manager.iterator()
                            .forEachRemaining(ea -> {
                                counter.incrementAndGet();
                            });
                   log.info("Delta: {}/{}", counter.get(), added.get());
                }catch(Exception e){
                    e.printStackTrace();
                }
            },0L, 100L, TimeUnit.MILLISECONDS);
        });


        IntStream.range(0, 10000000)
                .forEach(i -> {
                    try {
                        long id = Sequencer.SHARED.tick();
                        manager.append(new Event(id,
                                "foo".getBytes(StandardCharsets.UTF_8),
                                "bar".getBytes(StandardCharsets.UTF_8),
                                "baz".getBytes(StandardCharsets.UTF_8)));
                        added.incrementAndGet();
                    } catch (EventAppendException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });

        scheduler.shutdown();
        while(true){
            try {
                scheduler.awaitTermination(1L, TimeUnit.SECONDS);
                break;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        manager.close();

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
