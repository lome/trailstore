package org.lome.trailstore.storage.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FsWatcher implements AutoCloseable, Closeable {

    final WatchService watchService;
    final Path watchPath;
    final WatchKey watchKey;
    final Predicate<Path> fileFilter;
    final ConcurrentHashMap<String,Path> folderContentMirror;
    final Thread watchThread;
    final AtomicBoolean keepWatching;

    @SneakyThrows
    public FsWatcher(final Path watchPath, Predicate<Path> fileFilter){
        this.watchService = FileSystems.getDefault().newWatchService();
        this.watchPath = watchPath;
        this.watchKey = this.watchPath.register(this.watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.OVERFLOW);
        this.fileFilter = fileFilter;
        this.folderContentMirror = new ConcurrentHashMap<>();
        this.watchThread = new Thread(this::startWatching);
        this.keepWatching = new AtomicBoolean(true);
        initMap();
        this.watchThread.start();
    }

    public Stream<Path> folderContent(boolean reverse){
        return folderContentMirror.values()
                .stream()
                .sorted((p1,p2) -> {
                    return (reverse ? -1 : 1)
                            * p1.getFileName().toString()
                            .compareTo(p2.getFileName().toString());
                })
                .filter(Files::exists)
                .filter(Files::isReadable);
    }

    private void startWatching() {
        WatchKey key;
        while (keepWatching.get()) {
            try {
                try {
                    if ((key = watchService.poll(100, TimeUnit.MILLISECONDS)) == null) continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    switch (event.kind().name()) {
                        case "ENTRY_CREATE": {
                            Path target = this.watchPath.resolve((Path) event.context());
                            log.info("File created: {}", target);
                            addToMirror(target);
                        }
                        ;
                        break;
                        case "ENTRY_DELETE": {
                            Path target = this.watchPath.resolve((Path) event.context());
                            log.info("File removed: {}", target);
                            removedFromMirror(target);
                        }
                        ;
                        break;
                        case "ENTRY_MODIFY": {
                            Path target = this.watchPath.resolve((Path) event.context());
                            log.trace("File updated: {}", target);
                            addToMirror(target);
                        }
                        ;
                        break;
                        case "OVERFLOW": {
                            log.warn("Fs Events Overflow!");
                            initMap();
                        }
                        default:
                            log.error("Unknown Event Kind: {}", event.kind());
                    }
                }
                key.reset();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    private void removedFromMirror(Path path) {
        folderContentMirror.put(path.toAbsolutePath().toString(),path);
    }

    private void addToMirror(Path path) {
        folderContentMirror.put(path.toAbsolutePath().toString(),path);
    }

    @SneakyThrows
    private void initMap() {
        Files.list(this.watchPath)
                .filter(this.fileFilter)
                .forEach(this::addToMirror);
    }

    @Override
    public void close() throws IOException {
        this.keepWatching.set(false);
        try {
            this.watchThread.join();
        } catch (InterruptedException e) {
            //Ignore
        }
        this.watchKey.cancel();
    }
}
