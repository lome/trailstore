package org.lome.trailstore.storage.chunks;

import com.google.common.cache.*;
import jdk.jshell.spi.ExecutionControl;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.exceptions.EventReadException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.mvwal.MvWal;
import org.lome.trailstore.storage.utils.FsWatcher;
import org.lome.trailstore.utils.Sequencer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class ChunkManager implements AutoCloseable, Closeable, ChunkReader {

    final static long MAX_MEMORY_EVENTS = 1000000L; //1M events in memory
    final static String CHUNK_PATTERN = "^[0-9]+\\.CHUNK";

    final FsWatcher fsWatcher;
    final Path chunkFolder;

    final LoadingCache<Path,Chunker> chunkReaders;
    final ScheduledExecutorService storageExecutor = Executors.newSingleThreadScheduledExecutor();
    final MvWal walManager;

    MemoryChunk currentMemoryChunk;

    final LinkedBlockingQueue<MemoryChunk> chunkStoreQueue;


    public ChunkManager(Path chunkFolder, Path walFolder) throws IOException {
        this.chunkFolder = chunkFolder;
        Files.createDirectories(this.chunkFolder);
        this.fsWatcher = new FsWatcher(chunkFolder, (p) -> Files.isRegularFile(p) &&
                p.getFileName().toString().toUpperCase().matches(CHUNK_PATTERN));
        this.walManager = new MvWal(walFolder);
        this.currentMemoryChunk = new MemoryChunk();
        this.chunkStoreQueue = new LinkedBlockingQueue<>();
        this.chunkReaders = CacheBuilder.newBuilder()
                .maximumSize(50)
                .expireAfterAccess(6L, TimeUnit.HOURS)
                .build(new CacheLoader<Path, Chunker>() {
                    @Override
                    public Chunker load(Path key) throws Exception {
                        return new Chunker(key);
                    }
                });
        this.storageExecutor.scheduleWithFixedDelay(() -> {
            try{
                while(true) {
                    try (MemoryChunk chunk = chunkStoreQueue.poll()) {
                        if (chunk == null) break;
                        storeChunk(chunk);
                    }
                }
            }catch(Exception e){
                log.error("Error during chunkStorage routing",e);
            }
        }, 100L, 100L, TimeUnit.MILLISECONDS);
        //Reload missing items from WAL
        reloadFromWal();
    }

    @Override
    public Stream<EventAccessor> eventStream() throws ChunkClosedException {
        return StreamSupport.stream(new Iterable<EventAccessor>(){
            @Override
            public Iterator<EventAccessor> iterator() {
                try {
                    return eventIterator();
                } catch (ChunkClosedException e) {
                    throw new RuntimeException(e);
                }
            }

        }.spliterator(),false);
    }

    @Override
    public Iterator<EventAccessor> eventIterator() throws ChunkClosedException {
        return new ChunkManagerEventsIterator(this);
    }

    @Override
    public Stream<Long> idStream() throws ChunkClosedException {
        return eventStream().map(ea -> ea.getId());
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public ChunkInfo info() throws ChunkClosedException {
        //Too Expensive
        throw new RuntimeException("Not implemented");
    }

    @Getter
    public static class Chunker implements Comparable<Chunker>{
        final ChunkInfo info;
        final Path chunkPath;
        Chunker(Path chunkPath) throws IOException, ChunkClosedException {
            this.chunkPath = chunkPath;
            ChunkReader reader = new ChunkFileReader(chunkPath.toFile());
            this.info = reader.info();
        }
        boolean isValid(){
            return Files.exists(this.chunkPath)
                    && Files.isRegularFile(this.chunkPath)
                    && Files.isReadable(this.chunkPath);
        }

        @Override
        public int compareTo(Chunker chunker) {
            return this.chunkPath.getFileName().toString()
                    .compareTo(chunker.getChunkPath().getFileName().toString());
        }

        @SneakyThrows
        public ChunkReader reader(){
            return new ChunkFileReader(this.chunkPath.toFile());
        }
    }

    private int reloadFromWal(){
        log.info("Reloading WAL events");
        long lastStored = getLastStoredTick();
        long nowTick = Sequencer.SHARED.fixedTick(System.currentTimeMillis());
        walManager.snapshotStream(lastStored,nowTick,false)
                .filter(e -> e.getId() > lastStored) //Filter first event
                .forEach(e -> {
                    currentMemoryChunk.append(e);
                    try {
                        checkSwap();
                    } catch (Exception ex) {
                        log.error("Error during WAL reload",e);
                    }
                });
        int reloaded = currentMemoryChunk.size();
        log.info("Reloaded {} events from WAL",reloaded);
        return reloaded;
    }

    private Stream<Chunker> storedChunks(boolean reverse){
        return fsWatcher.folderContent(reverse)
                .map(p -> {
                    try {
                        return this.chunkReaders.get(p);
                    } catch (ExecutionException e) {
                        log.error("Error loading Chunker from {}",p,e);
                        return null;
                    }
                }).filter(c -> c != null && c.isValid());
    }

    protected TreeSet<Chunker> storedChunksSet(){
        TreeSet<Chunker> chunkers = new TreeSet<>();
        storedChunks(false).forEach(chunkers::add);
        return chunkers;
    }

    private long getLastStoredTick(){
        return storedChunks(true)
                .mapToLong(c -> c.getInfo().getLast())
                .findFirst().orElse(0);
    }


    private boolean isChunk(Path child) {
        return child.toFile().getName().matches("^[0-9]+\\.chunk$");
    }

    @SneakyThrows
    public void close(){
        this.storageExecutor.shutdown();
        while(!this.storageExecutor.awaitTermination(100L, TimeUnit.MILLISECONDS));
        this.currentMemoryChunk.close();
        this.walManager.close();
        this.fsWatcher.close();
    }

    public synchronized void append(Event event) throws EventAppendException {
        walManager.append(event);
        currentMemoryChunk.append(event);
        try {
            checkSwap();
        } catch (Exception e) {
            throw new EventAppendException(e);
        }
    }

    private void checkSwap() throws IOException, ExecutionException, InterruptedException {
        //Check size && Roll if needed
        if (currentMemoryChunk.size() >= MAX_MEMORY_EVENTS){
            MemoryChunk filledMemoryChunk = currentMemoryChunk;
            currentMemoryChunk = new MemoryChunk();
            chunkStoreQueue.put(filledMemoryChunk);
        }
    }

    private boolean storeChunk(final MemoryChunk chunk){
        ChunkInfo info = null;
        try {
            info = chunk.info();
        } catch (ChunkClosedException e) {
            log.error("Storing a closed chunk!");
            return false;
        }
        long first = info.getFirst();
        Path chunkFile = Path.of(chunkFolder.toString(),String.format("%d.chunk",first));
        while (Files.exists(chunkFile)){
            log.error("File {} already exists.. something's wrong here!",chunkFile);
            first++;
            chunkFile = Path.of(chunkFolder.toString(),String.format("%d.chunk",first));
        }
        log.info("Storing memory chunk as {}",chunkFile);
        try {
            chunk.store(chunkFile.toFile());
            log.info("Stored memory chunk as {}",chunkFile);
        }catch(IOException e){
            log.error("Error storing chunk file {}",chunkFile,e);
            try{
                Files.delete(chunkFile);
            }catch (Exception de){
                //Ignore
            }
            return false;
        }finally {
            try {
                chunk.close();
            } catch (IOException e) {
                log.error("Error closing Chunk",e);
            }
        }
        log.info("Removing WAL entries {}/{}",info.getFirst(),info.getLast());
        walManager.remove(info.getFirst(),info.getLast());
        log.info("Removed WAL entries {}/{}",info.getFirst(),info.getLast());
        return true;
    }


}
