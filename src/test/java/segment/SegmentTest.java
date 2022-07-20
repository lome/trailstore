package segment;

import org.junit.jupiter.api.Test;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.segment.ArrowMemorySegment;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SegmentTest {

    final static Logger log = LoggerFactory.getLogger(SegmentTest.class);

    @Test
    public void testSegment() throws IOException {
        ArrowMemorySegment segment = new ArrowMemorySegment();
        segment.append(new Event(Sequencer.SHARED.tick(),
                "foo".getBytes(StandardCharsets.UTF_8),
                "bar".getBytes(StandardCharsets.UTF_8),
                "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, segment.rows());
        segment.close();
    }

    @Test
    public void testSegment2() throws IOException {
        ArrowMemorySegment segment = new ArrowMemorySegment();
        for (int j=0;j < 100; j++)
        segment.append(new Event(Sequencer.SHARED.tick(),
                "foo".getBytes(StandardCharsets.UTF_8),
                "bar".getBytes(StandardCharsets.UTF_8),
                "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(100, segment.rows());
        AtomicInteger counter = new AtomicInteger();
        AtomicLong seq = new AtomicLong(0);
        segment.iterator().forEachRemaining(e -> {
            counter.incrementAndGet();
            assertTrue(seq.getAndSet(e.getId()) < e.getId());
        });
        assertEquals(100, counter.get());
        segment.close();
    }

    @Test
    public void testSegment3() throws IOException {
        ArrowMemorySegment segment = new ArrowMemorySegment();
        for (int j=0;j < 25; j++)
            segment.append(new Event(Sequencer.SHARED.tick(),
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8),
                    "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(25, segment.rows());
        AtomicInteger counter = new AtomicInteger();
        segment.iterator().forEachRemaining(e -> {
            counter.incrementAndGet();
        });
        assertEquals(25, counter.get());
        for (int j=0;j < 25; j++)
            segment.append(new Event(Sequencer.SHARED.tick(),
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8),
                    "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(50, segment.rows());
        segment.iterator().forEachRemaining(e -> {
            counter.incrementAndGet();
        });
        assertEquals(75, counter.get());
        for (int j=0;j < 25; j++)
            segment.append(new Event(Sequencer.SHARED.tick(),
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8),
                    "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(75, segment.rows());
        segment.iterator().forEachRemaining(e -> {
            counter.incrementAndGet();
        });
        assertEquals(150, counter.get());
        for (int j=0;j < 25; j++)
            segment.append(new Event(Sequencer.SHARED.tick(),
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8),
                    "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(100, segment.rows());
        segment.iterator().forEachRemaining(e -> {
            counter.incrementAndGet();
        });
        assertEquals(250, counter.get());
        segment.close();
    }

    @Test
    public void testSegment4() throws IOException, InterruptedException {
        ArrowMemorySegment segment = new ArrowMemorySegment();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(40);
        AtomicInteger counter = new AtomicInteger();
        for (int t=0; t < 40; t++)
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    AtomicInteger lcounter = new AtomicInteger();
                    segment.iterator().forEachRemaining(e -> {
                        lcounter.incrementAndGet();
                    });
                    assertEquals(counter.get(), lcounter.get());
                }catch(Exception e){
                    e.printStackTrace();
                }
            }, 100, 100, TimeUnit.MILLISECONDS);
        for (int j=0;j < 1000000; j++)
            segment.append(new Event(Sequencer.SHARED.tick(),
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8),
                    "baz".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1000000, segment.rows());

        segment.iterator().forEachRemaining(e -> {
            counter.incrementAndGet();
        });
        assertEquals(1000000, counter.get());
        scheduler.shutdown();
        while(!scheduler.awaitTermination(100, TimeUnit.MILLISECONDS));
        segment.close();
    }

}
