package chunks;
import org.junit.jupiter.api.Test;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.chunks.ChunkClosedException;
import org.lome.trailstore.storage.chunks.ChunkManager;
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
import java.util.Stack;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class ChunksTest {

    final static Logger log = LoggerFactory.getLogger(ChunksTest.class);

    @Test
    public void perfWrite() throws IOException, ChunkClosedException {
        clear(Path.of("chunks"));
        clear(Path.of("wals"));

        ChunkManager manager = new ChunkManager(Path.of("chunks"),Path.of("wals"));
        Set<Long> idStack = new HashSet<>();
        IntStream.range(0, 5000012)
                .forEach(i -> {
                    try {
                        long id = Sequencer.SHARED.tick();
                        idStack.add(id);
                        manager.append(new Event(id,
                                "foo".getBytes(StandardCharsets.UTF_8),
                                "bar".getBytes(StandardCharsets.UTF_8),
                                "baz".getBytes(StandardCharsets.UTF_8)));
                    } catch (EventAppendException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
        manager.eventStream()
                        .forEach(ea -> {
                            assertEquals(true,idStack.remove(ea.getId()));
                        });
        manager.close();

        assertEquals(0,idStack.size());

        clear(Path.of("chunks"));
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
