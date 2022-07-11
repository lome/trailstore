package org.lome.trailstore.storage.wal;

import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.model.Event;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;

@Slf4j
public class WalSnapshotScanner implements WalScanner{

        Path currentWalFile;
        WalFileScanner currentScanner;
        final LinkedList<Path> walFiles;
        final long firstTick;
        final long lastTick;

        Event lookAhead;

        public WalSnapshotScanner(LinkedList<Path> walFiles,
                                  long firstTick,
                                  long lastTick){
            this.walFiles = walFiles;
            this.firstTick = firstTick;
            this.lastTick = lastTick;
            nextWalFile();
        }

        @Override
        public void reset() {
            currentWalFile = null;
            currentScanner = null;
            nextWalFile();
        }

        @Override
        public void close() throws IOException {
            if (currentScanner != null){
                currentScanner.close();
                currentScanner = null;
                currentWalFile = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (currentWalFile == null || currentScanner == null)
                return false;

            //Still there ?
            while(walFiles.contains(currentWalFile) && currentScanner.hasNext()){
                this.lookAhead = currentScanner.next();
                if (lookAhead.getId() >= this.firstTick && lookAhead.getId() < this.lastTick){
                    return true;
                }
            }
            nextWalFile();
            return hasNext();
        }

        protected void nextWalFile() {
            if (currentScanner != null){
                currentScanner.close();
            }
            if (walFiles.isEmpty() || walFiles.getLast().equals(currentWalFile)){
                currentWalFile = null;
                currentScanner = null;
                return;
            }
            Path nextWalFile = currentWalFile == null ?
                    walFiles.getFirst()
                    : walFiles.get(walFiles.indexOf(currentWalFile)+1);
            currentWalFile = nextWalFile;
            try {
                currentScanner = new WalFileScanner(this.currentWalFile);
            } catch (IOException e) {
                log.error("Error creating scanner for wal file {}",this.currentWalFile,e);
                nextWalFile();
            }
        }

        @Override
        public Event next() {
            return lookAhead;
        }

        protected Path getCurrentWalFile(){
            return this.currentWalFile;
        }

}
