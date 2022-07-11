package org.lome.trailstore.storage.chunks;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.exceptions.EventReadException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.wal.*;
import org.lome.trailstore.utils.Sequencer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@Slf4j
public class ChunkManager implements AutoCloseable {

    final static long MAX_MEMORY_EVENTS = 1000000L; //1M events in memory

    LinkedList<Path> chunkFiles;
    FileStore filestore;
    final Path chunkFolder;

    MemoryChunk currentMemoryChunk;
    WalManager walManager;

    long lastTick;

    public ChunkManager(Path chunkFolder, Path walFolder) throws IOException {
        this.chunkFolder = chunkFolder;
        this.chunkFiles = new LinkedList<>();
        this.walManager = new WalManager(walFolder);
        this.currentMemoryChunk = new MemoryChunk();
        init();
    }

    @SneakyThrows
    private void init(){
        Files.createDirectories(this.chunkFolder);
        this.filestore = Files.getFileStore(this.chunkFolder);
        Files.list(this.chunkFolder)
                .filter(this::isChunk)
                .sorted((p1,p2) -> {
                    return p1.getFileName().compareTo(p2.getFileName());
                })
                .forEach(child -> {
                    log.debug("Chunk found: {}",child);
                    //Read WALs are frozen.
                    chunkFiles.add(child);
                });
        reloadFromWal();
    }

    private void reloadFromWal(){
        log.info("Reloading WAL events");
        walManager.snapshot(getLastStoredTick(),Sequencer.SHARED.fixedTick(System.currentTimeMillis()))
                .forEachRemaining(e -> {
                    currentMemoryChunk.append(e);
                    checkPersistence();
                });
        log.info("Reloaded {} events from WAL",currentMemoryChunk.size());
    }

    private long getLastStoredTick(){
        if (this.chunkFiles.isEmpty()) return 0;
        return this.chunkFiles.stream()
                .map(Path::toFile)
                .map(file -> {
                    try {
                        return new ChunkFileReader(file);
                    } catch (IOException e) {
                       log.error("Error opening file {}",file,e);
                       return null;
                    }
                }).filter(Objects::nonNull)
                .map(r -> {
                    try {
                        return r.info();
                    } catch (ChunkClosedException e) {
                        log.error("Error getting info for reader",e);
                        return null;
                    }
                }).filter(Objects::nonNull)
                .mapToLong(ChunkInfo::getLast)
                .max().getAsLong();
    }


    private boolean isChunk(Path child) {
        return child.toFile().getName().matches("^[0-9]+\\.chunk$");
    }

    @SneakyThrows
    public void close(){
        this.currentMemoryChunk.close();
    }

    public void append(Event event) throws EventAppendException {
        walManager.append(event);
        currentMemoryChunk.append(event);
        checkPersistence();
    }

    @SneakyThrows
    private void checkPersistence() {
        //Check size && Roll if needed
        if (currentMemoryChunk.size() >= MAX_MEMORY_EVENTS){
            ChunkInfo info = currentMemoryChunk.info();
            long first = info.getFirst();
            Path chunkFile = Path.of(chunkFolder.toString(),String.format("%d.chunk",first));
            while (Files.exists(chunkFile)){
                log.error("File {} already exists.. something's wrong here!");
                first++;
                chunkFile = Path.of(chunkFolder.toString(),String.format("%d.chunk",first));
            }
            log.info("Storing memory chunk as {}",chunkFile);
            try {
                currentMemoryChunk.store(chunkFile.toFile());
                log.info("Stored memory chunk as {}",chunkFile);
                chunkFiles.add(chunkFile);
            }catch(IOException e){
                log.error("Error storing chunk file {}",chunkFile,e);
                throw new EventAppendException("Error storing chunk file");
            }
            try{
                log.info("Truncating WAL at {}",info.getLast());
                walManager.truncate(info.getLast());
                log.info("WAL truncated at {}",info.getLast());
            }catch(IOException e){
                log.error("Error truncating WAL",e);
            }
            //Clear Memory chunks
            currentMemoryChunk = new MemoryChunk();
        }
    }

    public static void main(String[] args) throws IOException {
        ChunkManager appender = new ChunkManager(Path.of("chunks"),Path.of("wals"));
        IntStream.range(0, 5000000)
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
    }


}
