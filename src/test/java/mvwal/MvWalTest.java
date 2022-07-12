package mvwal;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.mvwal.MvWal;
import org.lome.trailstore.utils.Sequencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Stack;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class MvWalTest {

    final static Logger log = LoggerFactory.getLogger(MvWalTest.class);

    @Test
    public void testOne(){
        MvWal mvwal = new MvWal(Path.of("wals"));
        long id = Sequencer.SHARED.tick();
        mvwal.append(new Event(id,"key".getBytes(),"meta".getBytes(),"data".getBytes()));
        assertTrue(mvwal.events(false).hasNext());
        assertEquals(mvwal.events(false).next().getId(),id);
        assertArrayEquals(mvwal.events(false).next().getKey(),"key".getBytes());
        assertArrayEquals(mvwal.events(false).next().getMetadata(),"meta".getBytes());
        assertArrayEquals(mvwal.events(false).next().getData(),"data".getBytes());
        mvwal.truncate(id);
        assertFalse(mvwal.events(false).hasNext());
        close(mvwal);
    }

    void close(MvWal wal){
        try{
         wal.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testSequence(){
        MvWal mvwal = new MvWal(Path.of("wals"));
        Stack<Long> given = new Stack<>();
        IntStream.range(0,100)
                .forEach(i -> {
                    Event event = new Event(Sequencer.SHARED.tick(),
                            "key".getBytes(),"meta".getBytes(),"data".getBytes());
                    mvwal.append(event);
                    given.push(event.getId());
                });

        mvwal.eventStream(true)
                .forEach(e -> {
                    assertEquals(e.getId(),given.pop());
                });

        mvwal.truncate(Sequencer.SHARED.tick());

        close(mvwal);
    }

    @Test
    public void testDuplicateForbidden(){
        MvWal mvwal = new MvWal(Path.of("wals"));
        Event event = new Event(Sequencer.SHARED.tick(),
                "key".getBytes(),"meta".getBytes(),"data".getBytes());
        mvwal.append(event);
        assertThrows(EventAppendException.class, () -> {
            mvwal.append(event);
        });
        mvwal.truncate(Sequencer.SHARED.tick());
        close(mvwal);
    }

    @Test
    public void testWritePerf(){
        long start = System.currentTimeMillis();
        MvWal mvwal = new MvWal(Path.of("wals"));
        final int EVENTS = 5000000;
        IntStream.range(0,EVENTS)
                .forEach(i -> {
                    Event event = new Event(Sequencer.SHARED.tick(),
                            "key".getBytes(),"meta".getBytes(),"data".getBytes());
                    mvwal.append(event);
                });
        double elapsed = (System.currentTimeMillis()-start)/1000.0;
        double evSec = EVENTS/elapsed;
        log.info("Write Rate: {}",evSec);
        start = System.currentTimeMillis();
        mvwal.eventStream(false).forEach(e -> log.trace("{}",e));
        elapsed = (System.currentTimeMillis()-start)/1000.0;
        evSec = EVENTS/elapsed;
        log.info("Read Rate: {}",evSec);
        mvwal.truncate(Sequencer.SHARED.tick());
        close(mvwal);
    }

}
