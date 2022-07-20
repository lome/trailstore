package org.lome.trailstore.storage.segment;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.exceptions.EventAppendException;
import org.lome.trailstore.model.Event;
import org.lome.trailstore.storage.mvwal.MvWal;
import org.lome.trailstore.storage.utils.FsWatcher;
import org.lome.trailstore.utils.Sequencer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class SegmentManager implements Closeable {

    final static long MAX_MEMORY_EVENTS = 1000000L; //1M events in memory
    final static String SEGMENT_PATTERN = "^[0-9]+\\.SEGMENT";

    final FsWatcher fsWatcher;
    final Path segmentsFolder;

    final LoadingCache<Path,ArrowFileSegment> fileSegments;
    final ScheduledExecutorService storageExecutor = Executors.newSingleThreadScheduledExecutor();
    final MvWal walManager;

    ArrowMemorySegment currentMemorySegment;
    final LinkedBlockingQueue<ArrowMemorySegment> segmentStoreQueue;
    final TreeSet<EventIteratorFactory> readerSegments;


    public SegmentManager(Path segmentsFolder, Path walFolder) throws IOException {
        this.segmentsFolder = segmentsFolder;
        Files.createDirectories(this.segmentsFolder);
        this.fsWatcher = new FsWatcher(segmentsFolder, (p) -> Files.isRegularFile(p) &&
                p.getFileName().toString().toUpperCase().matches(SEGMENT_PATTERN));
        this.walManager = new MvWal(walFolder);
        this.currentMemorySegment = new ArrowMemorySegment();
        this.segmentStoreQueue = new LinkedBlockingQueue<>();
        this.fileSegments = CacheBuilder.newBuilder()
                .maximumSize(50)
                .expireAfterAccess(6L, TimeUnit.HOURS)
                .build(new CacheLoader<Path, ArrowFileSegment>() {
                    @Override
                    public ArrowFileSegment load(Path key) throws Exception {
                        return new ArrowFileSegment(key.toFile());
                    }
                });
        this.readerSegments = new TreeSet<>();
        availableSegments()
                .stream().map(EventIteratorFactory::new)
                .forEach(this.readerSegments::add);
        this.storageExecutor.scheduleWithFixedDelay(() -> {
            try{
                while(true) {
                    try (ArrowMemorySegment segment = segmentStoreQueue.poll()) {
                        if (segment == null) break;
                        ArrowFileSegment fileSegment = storeSegment(segment);
                        if (fileSegment != null){
                            try {
                                readerSegments.stream()
                                        .filter(e -> e.getSegment().equals(segment))
                                        .findFirst().get().swapSource(fileSegment);
                            }catch(Exception e){
                                e.printStackTrace();
                            }
                        }else{
                            segmentStoreQueue.put(segment);
                        }
                    }
                }
            }catch(Exception e){
                log.error("Error during chunkStorage routing",e);
            }
        }, 100L, 100L, TimeUnit.MILLISECONDS);
        //Reload missing items from WAL
        reloadFromWal();
    }

    private int reloadFromWal(){
        log.info("Reloading WAL events");
        long lastStored = getLastStoredTick();
        long nowTick = Sequencer.SHARED.fixedTick(System.currentTimeMillis());
        AtomicInteger reloaded = new AtomicInteger(0);
        walManager.snapshotStream(lastStored,nowTick,false)
                .filter(e -> e.getId() > lastStored) //Filter first event
                .forEach(e -> {
                    currentMemorySegment.append(e);
                    reloaded.incrementAndGet();
                    try {
                        checkSwap();
                    } catch (Exception ex) {
                        log.error("Error during WAL reload",e);
                    }
                });
        log.info("Reloaded {} events from WAL",reloaded.get());
        return reloaded.get();
    }

    private Stream<ArrowFileSegment> storedSegments(boolean reverse){
        return fsWatcher.folderContent(reverse)
                .map(p -> {
                    try {
                        return this.fileSegments.get(p);
                    } catch (ExecutionException e) {
                        log.error("Error loading Segment from {}",p,e);
                        return null;
                    }
                }).filter(c -> c != null);
    }

    private TreeSet<ArrowFileSegment> sortedStoredSegments(){
        TreeSet<ArrowFileSegment> segments = new TreeSet<>();
        storedSegments(false).forEach(segments::add);
        return segments;
    }

    private TreeSet<ArrowSegment> availableSegments(){
        TreeSet<ArrowSegment> segments = new TreeSet<>();
        segments.addAll(sortedStoredSegments());
        segments.addAll(segmentStoreQueue.stream().toList());
        segments.add(currentMemorySegment);
        return segments;
    }

    private long getLastStoredTick(){
        return storedSegments(true)
                .mapToLong(c -> c.last())
                .findFirst().orElse(0);
    }

    public Iterator<EventAccessor> iterator(){
        //log.info("Iterator from {} segments", readerSegments.size());
        if (readerSegments.isEmpty()) {
            log.info("Segments are empty.");
            return new ArrayList<EventAccessor>().iterator();
        }

        return new Iterator<EventAccessor>() {
            AtomicInteger segCount = new AtomicInteger(1);
            EventIteratorFactory currentFactory = readerSegments.first();
            Iterator<EventAccessor> current = currentFactory.newIterator();

            @Override
            public boolean hasNext() {
                if (current.hasNext()) return true;
                currentFactory = readerSegments.higher(currentFactory);
                if (currentFactory != null){
                    current = currentFactory.newIterator();
                    segCount.incrementAndGet();
                    return hasNext();
                }else{
                    //log.info("Traversed segments: {}",segCount.get());
                    return false;
                }
            }

            @Override
            public EventAccessor next() {
                return current.next();
            }
        };
    }

    @SneakyThrows
    public void close(){
        this.storageExecutor.shutdown();
        while(!this.storageExecutor.awaitTermination(100L, TimeUnit.MILLISECONDS));
        this.currentMemorySegment.close();
        this.walManager.close();
        this.fsWatcher.close();
    }

    public synchronized void append(Event event) throws EventAppendException {
        walManager.append(event);
        currentMemorySegment.append(event);
        try {
            checkSwap();
        } catch (Exception e) {
            throw new EventAppendException(e);
        }
    }

    private void checkSwap() throws IOException, ExecutionException, InterruptedException {
        //Check size && Roll if needed
        if (currentMemorySegment.rows() >= MAX_MEMORY_EVENTS){
            ArrowMemorySegment filledMemorySegment = currentMemorySegment;
            currentMemorySegment = new ArrowMemorySegment();
            segmentStoreQueue.put(filledMemorySegment);
            readerSegments.add(new EventIteratorFactory(currentMemorySegment));
        }
    }

    private ArrowFileSegment storeSegment(final ArrowMemorySegment segment) throws IOException {
        long first = segment.first();
        long last = segment.last();
        Path segmentFile = Path.of(segmentsFolder.toString(),String.format("%d.segment",first));
        while (Files.exists(segmentFile)){
            log.error("File {} already exists.. something's wrong here!",segmentFile);
            first++;
            segmentFile = Path.of(segmentsFolder.toString(),String.format("%d.segment",first));
        }
        log.info("Storing memory segment as {}",segmentFile);
        try {
            segment.store(segmentFile.toFile());
            log.info("Stored memory segment as {}",segmentFile);
        }catch(IOException e){
            log.error("Error storing segment file {}",segmentFile,e);
            try{
                Files.delete(segmentFile);
            }catch (Exception de){
                //Ignore
            }
            return null;
        }
        log.info("Removing WAL entries {}/{}",first,last);
        walManager.remove(first,last);
        log.info("Removed WAL entries {}/{}",first,last);
        segment.close();
        return new ArrowFileSegment(segmentFile.toFile());
    }


}
