package org.lome.trailstore.storage.wal;

import lombok.extern.slf4j.Slf4j;
import org.lome.trailstore.model.Event;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;

@Slf4j
public class WalManagerScanner implements WalScanner{

        Path currentWalFile;
        WalFileScanner currentScanner;
        final LinkedList<Path> walFiles;

        public WalManagerScanner(LinkedList<Path> walFiles){
            this.walFiles = walFiles;
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
            if (walFiles.contains(currentWalFile) && currentScanner.hasNext()){
                return true;
            }else{
                nextWalFile();
                return hasNext();
            }
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
            return currentScanner == null ? null
                    : currentScanner.next();
        }

        protected Path getCurrentWalFile(){
            return this.currentWalFile;
        }

}
