package org.lome.trailstore.storage.wal;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.exceptions.EventReadException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.utils.Sequencer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@Slf4j
public class WalManager implements AutoCloseable, WalScanner, WalAppender{

    final static long MAX_LOG_SIZE = (long)(20*(Math.pow(1024,2))); //20 MBytes
    final static long GUARD = (long)(256); //Guard

    LinkedList<Path> walFiles;
    FileStore filestore;
    final Path walFolder;

    WalFileAppender currentAppender;
    WalManagerScanner walScanner;

    long lastTick;

    public WalManager(Path walFolder){
        this.walFolder = walFolder;
        this.walFiles = new LinkedList<>();
        init();
        readLastTick();
    }

    private void readLastTick() {
        if (hasNext()){
            lastTick = next().getId();
            reset();
        }else{
            lastTick = -1;
        }
    }

    @SneakyThrows
    private void init(){
        Files.createDirectories(this.walFolder);
        this.filestore = Files.getFileStore(this.walFolder);
        Files.list(this.walFolder)
                .filter(this::isWal)
                .sorted((p1,p2) -> {
                    return p1.getFileName().compareTo(p2.getFileName());
                })
                .forEach(child -> {
                    log.debug("WAL found: {}",child);
                    //Read WALs are frozen.
                    walFiles.add(child);
                });
        this.walScanner = new WalManagerScanner(this.walFiles);
        initAppender();
    }

    private void initAppender() throws IOException {
        this.currentAppender = new WalFileAppender(Path.of(this.walFolder.toString(),
                String.valueOf(Sequencer.SHARED.tick())+".wal"));
    }

    public void flush() throws IOException {
        initAppender();
    }

    private boolean isWal(Path child) {
        return child.toFile().getName().matches("^[0-9]+\\.wal$");
    }

    @SneakyThrows
    public void close(){
        this.walScanner.close();
    }

    public WalScanner snapshot(long firstTick, long lastTick){
        LinkedList<Path> snapshot = new LinkedList<>(walFiles);
        if (this.currentAppender != null && !this.walFiles.contains(this.currentAppender.filePath)){
            snapshot.add(this.currentAppender.filePath);
        }
        LinkedList<Path> candidates = new LinkedList<>();
        snapshot.stream()
                .filter(walFile -> {
                    long logTick =
                            Long.parseLong(walFile.toFile().getName().replaceAll("\\.wal$",""));
                    return logTick < lastTick;
        }).forEach(candidates::add);
        return new WalSnapshotScanner(candidates,firstTick,lastTick);
    }

    /*
        This will RESET the manager!
     */
    public void truncate(long lastTick) throws IOException {
        if (this.currentAppender != null && !this.walFiles.contains(this.currentAppender.filePath)){
            this.walFiles.add(this.currentAppender.filePath);
        }
        initAppender();
        LinkedList<Path> snapshot = new LinkedList<>(walFiles);
        snapshot.stream()
                .forEach(walFile -> {
            long logTick =
                    Long.parseLong(walFile.toFile().getName().replaceAll("\\.wal$",""));
            if (logTick <= lastTick){
                log.debug("Working on {}",walFile);
                WalFileScanner scanner = null;
                try {
                    scanner = new WalFileScanner(walFile);
                } catch (IOException e) {
                    log.error("I/O Exception opening {}, skipping",walFile);
                    return;
                }
                try {
                    boolean deletedAny = false;
                    boolean appendedAny = false;
                    Path tmpWal = Files.createTempFile("tmp_wal",".wal");
                    WalFileAppender appender = new WalFileAppender(tmpWal);
                    int read = 0;
                    while(scanner.hasNext()){
                        Event ev = scanner.next();
                        read++;
                        if (ev.getId() < lastTick){
                            deletedAny = true;
                        }else{
                            if (deletedAny){
                                appendedAny = true;
                                appender.append(ev);
                            }
                        }
                    }
                    scanner.close();
                    appender.close();
                    if (deletedAny){
                        if (appendedAny) {
                            log.debug("Moving reduced wal file {} to {}",tmpWal,walFile);
                            Files.move(tmpWal, walFile, StandardCopyOption.REPLACE_EXISTING);
                        }else{
                            log.debug("Deleting old wal file {}",walFile);
                            Files.delete(walFile);
                            walFiles.remove(walFile);
                        }
                    }else{
                        if (read == 0){
                            log.debug("Deleting empty wal file {}",walFile);
                            Files.delete(walFile);
                            walFiles.remove(walFile);
                        }
                    }
                } catch (IOException e) {
                    log.error("I/O Exception while working on {}, skipping",walFile);
                    return;
                }
            }
        });
        // In any case, reset!
        this.reset();
    }

    @Override
    public void reset() throws EventReadException {
        this.walScanner.reset();
    }

    @Override
    public boolean hasNext() throws EventReadException  {
        return this.walScanner.hasNext();
    }

    @Override
    public Event next() throws EventReadException  {
        return this.walScanner.next();
    }

    @Override
    public void append(Event event) throws EventAppendException {
        ByteBuffer buf = event.toBuffer();
        if (currentAppender.size()+buf.remaining()+GUARD >= MAX_LOG_SIZE){
            log.debug("Rolling log file...");
            currentAppender.close();
            walFiles.add(currentAppender.filePath);
            try {
                initAppender();
            } catch (IOException e) {
                throw new EventAppendException(e);
            }
            append(event);
            return;
        }
        currentAppender.append(event);
    }

    public static void main(String[] args) throws IOException {
        WalManager manager = new WalManager(Path.of("wals"));
        WalAppender appender = manager;
        WalScanner scanner = manager;

        IntStream.range(0, 1000000)
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
                    //System.out.println(rows);
                });

        long checkpoint = Sequencer.SHARED.tick();

        AtomicLong snapStart = new AtomicLong();
        AtomicLong snapEnd = new AtomicLong();

        IntStream.range(0, 1000000)
                .forEach(i -> {
                    Event ev = new Event(Sequencer.SHARED.tick(),
                            "foo".getBytes(StandardCharsets.UTF_8),
                            "bar".getBytes(StandardCharsets.UTF_8),
                            "baz".getBytes(StandardCharsets.UTF_8));
                    if (i == 500) snapStart.set(ev.getId());
                    if (i == 1000) snapEnd.set(ev.getId());
                    try {
                        appender.append(ev);
                    } catch (EventAppendException e) {
                        throw new RuntimeException(e);
                    }
                    int rows = 0;
                    while(scanner.hasNext()){
                        scanner.next();
                        rows++;
                    }
                    //System.out.println(rows);
                });

        manager.truncate(checkpoint);

        while(scanner.hasNext()){
            Event next = scanner.next();
            if (next.getId() <= checkpoint){
                log.error("Error!!! {}",next.getId());
            }else{
                //log.info("{}",next);
            }
        }

        WalScanner snapshotScanner = manager.snapshot(snapStart.get(),snapEnd.get());

        int read = 0;
        while(snapshotScanner.hasNext()){
            read++;
            Event ev = snapshotScanner.next();
            log.trace("{} | {} | {}",
                    ev.getId(),
                    ev.getId() == snapStart.get() ? "X" : "",
                    ev.getId() == snapEnd.get() ? "X" : "");
        }
        log.info("Snapshot Scanned: {}",read);

        appender.close();
        scanner.close();

        WalManager m2 = new WalManager(Path.of("wals"));
        read = 0;
        while(m2.hasNext()){
            read++;
            m2.next();
        }
        log.info("Got {} events totals",read);
        m2.close();

    }
}
