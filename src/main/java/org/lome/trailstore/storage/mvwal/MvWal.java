package org.lome.trailstore.storage.mvwal;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.exceptions.EventReadException;
import org.lome.trailstore.model.Event;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class MvWal implements AutoCloseable, Closeable {

    final static String WAL_FILENAME = "trail_log.wal";
    final static ScheduledExecutorService commitExecutor = Executors.newSingleThreadScheduledExecutor();

    final Path walPath;
    final MVStore mvStore;
    final MVMap<Long,byte[]> eventMap;

    transient ScheduledFuture<?> commitFuture;

    @SneakyThrows
    public MvWal(Path walPath){
        this(walPath,false);
    }

    @SneakyThrows
    public MvWal(Path walPath, boolean readOnly){
        this.walPath = walPath;
        if (!Files.exists(walPath)){
            Files.createDirectories(walPath);
        }
        MVStore.Builder builder = new MVStore.Builder()
                .fileName(Path.of(this.walPath.toFile().getAbsolutePath(),WAL_FILENAME).toFile().getAbsolutePath())
                .cacheSize(16)
                .autoCommitDisabled();
        if (readOnly) builder = builder.readOnly();
        this.mvStore = builder.open();
        this.eventMap = this.mvStore.openMap(WAL_FILENAME);
    }

    @Override
    public void close() throws IOException {
        mvStore.close();
        commitExecutor.shutdown();
    }

    void checkAppendable(){
        if(this.mvStore.isClosed()){
            throw new EventAppendException("Store is closed");
        }
        if (this.mvStore.isReadOnly()){
            throw new EventAppendException("Store is read only");
        }
    }

    void checkReadable(){
        if(this.mvStore.isClosed()){
            throw new EventReadException("Store is closed");
        }
    }

    public void append(Event event){
        checkAppendable();
        Long lastKey = this.eventMap.lastKey();
        if (lastKey != null && lastKey >= event.getId()){
            throw new EventAppendException("Invalid event id: "+event.getId()+", last event was: "+lastKey);
        }
        byte[] existing = this.eventMap.putIfAbsent(event.getId(),event.toByteArray());
        if (existing != null){
            throw new EventAppendException("An event with the same id already exists");
        }
        futureCommit();
    }

    private synchronized void futureCommit() {
        if (this.commitFuture != null){
            this.commitFuture.cancel(false);
        }
        this.commitFuture = commitExecutor.schedule(() -> {
                    log.info("Performing scheduled commit");
                    this.eventMap.store.tryCommit();
                }
                ,100, TimeUnit.MILLISECONDS);
    }

    public void commit(){
        this.eventMap.store.commit();
    }

    public void truncate(long lastTick){
        checkAppendable();
        commit();
        this.eventMap.cursor(this.eventMap.firstKey(),lastTick,false)
                .forEachRemaining(this.eventMap::remove);
        commit();
        this.eventMap.store.compactMoveChunks();
    }

    public Iterator<Event> snapshot(long from, long to, boolean reverse){
        checkReadable();
        final Cursor<Long,byte[]> it = this.eventMap.cursor(from,to,reverse);
        return new Iterator<Event>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Event next() {
                it.next();
                return Event.fromByteArray(it.getValue());
            }
        };
    }

    public Stream<Event> snapshotStream(long from, long to, boolean reverse){
        final Iterator<Event> eventIterator = snapshot(from,to,reverse);
        return StreamSupport.stream(new Iterable<Event>(){
            @Override
            public Iterator<Event> iterator() {
                return eventIterator;
            }
        }.spliterator(),false);
    }

    public Iterator<Event> events(boolean reverse){
        Long firstKey = this.eventMap.firstKey();
        Long lastKey = this.eventMap.lastKey();
        if (firstKey == null || lastKey == null){
            //Empty
            return new ArrayList<Event>().iterator();
        }
        return snapshot(this.eventMap.firstKey(),this.eventMap.lastKey(),reverse);
    }

    public Stream<Event> eventStream(boolean reverse){
        Long firstKey = this.eventMap.firstKey();
        Long lastKey = this.eventMap.lastKey();
        if (firstKey == null || lastKey == null){
            //Empty
            return new ArrayList<Event>().stream();
        }
        return snapshotStream(firstKey,lastKey,reverse);
    }

}
