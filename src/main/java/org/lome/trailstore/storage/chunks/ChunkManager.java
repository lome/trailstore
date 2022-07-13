package org.lome.trailstore.storage.chunks;

import com.google.common.cache.*;
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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class ChunkManager implements AutoCloseable, Closeable {

    final static long MAX_MEMORY_EVENTS = 1000000L; //1M events in memory
    final static String CHUNK_PATTERN = "^[0-9]+\\.CHUNK";

    final FsWatcher fsWatcher;
    final Path chunkFolder;

    final LoadingCache<Path,Chunker> chunkReaders;
    final ScheduledExecutorService storageExecutor = Executors.newSingleThreadScheduledExecutor();
    final MvWal walManager;

    MemoryChunk currentMemoryChunk;
    // Swap Utils
    Future<?> lastSwapFuture;


    public ChunkManager(Path chunkFolder, Path walFolder) throws IOException {
        this.chunkFolder = chunkFolder;
        Files.createDirectories(this.chunkFolder);
        this.fsWatcher = new FsWatcher(chunkFolder, (p) -> Files.isRegularFile(p) &&
                p.getFileName().toString().toUpperCase().matches(CHUNK_PATTERN));
        this.walManager = new MvWal(walFolder);
        this.currentMemoryChunk = new MemoryChunk();
        this.chunkReaders = CacheBuilder.newBuilder()
                .maximumSize(50)
                .expireAfterAccess(6L, TimeUnit.HOURS)
                .removalListener(new RemovalListener<Path, Chunker>() {
                    @Override
                    public void onRemoval(RemovalNotification<Path, Chunker> notification) {
                        try {
                            notification.getValue().getReader().close();
                        } catch (IOException e) {
                            log.error("Error closing {}",notification.getKey(),e);
                        }
                    }
                })
                .build(new CacheLoader<Path, Chunker>() {
                    @Override
                    public Chunker load(Path key) throws Exception {
                        return new Chunker(key);
                    }
                });

        //Reload missing items from WAL
        reloadFromWal();
    }

    @Getter
    public static class Chunker{
        final ChunkInfo info;
        final ChunkReader reader;
        final Path chunkPath;
        Chunker(Path chunkPath) throws IOException, ChunkClosedException {
            this.chunkPath = chunkPath;
            this.reader = new ChunkFileReader(chunkPath.toFile());
            this.info = this.reader.info();
        }
        boolean isValid(){
            return Files.exists(this.chunkPath)
                    && Files.isRegularFile(this.chunkPath)
                    && Files.isReadable(this.chunkPath)
                    && !reader.isClosed();
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
        this.currentMemoryChunk.close();
        this.walManager.close();
        this.storageExecutor.shutdown();
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
            // Am I still saving the old swapMemChunk ?\
            if (this.lastSwapFuture != null && !this.lastSwapFuture.isDone()){
                //Wait for it!
                this.lastSwapFuture.get();
            }
            MemoryChunk swapMemoryChunk = currentMemoryChunk;
            currentMemoryChunk = new MemoryChunk();
            this.lastSwapFuture = storeChunk(swapMemoryChunk);
        }
    }

    private Future<?> storeChunk(final MemoryChunk chunk){
        return storageExecutor.submit(() -> {
            ChunkInfo info = null;
            try {
                info = chunk.info();
            } catch (ChunkClosedException e) {
                log.error("Storing a closed chunk!");
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
                throw new EventAppendException("Error storing chunk file");
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
        });
    }

    public static void main(String[] args) throws IOException {
        ChunkManager appender = new ChunkManager(Path.of("chunks"),Path.of("wals"));
        long start = System.currentTimeMillis();
        IntStream.range(0, 5000012)
                .forEach(i -> {
                    try {
                        appender.append(new Event(Sequencer.SHARED.tick(),
                                "foo".getBytes(StandardCharsets.UTF_8),
                                "bar".getBytes(StandardCharsets.UTF_8),
                                "baz".getBytes(StandardCharsets.UTF_8)));
                    } catch (EventAppendException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
        double elapsed = (System.currentTimeMillis()-start)/1000.0;
        System.out.println("Write Perf: "+(5000000/elapsed)+" ev/sec");
        appender.close();
    }


}
