package org.lome.trailstore.storage.segment;

import com.google.common.collect.ConcurrentHashMultiset;
import io.netty.util.internal.ConcurrentSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class EventIteratorFactory implements Comparable<EventIteratorFactory>{

    @Getter
    ArrowSegment segment;
    final ConcurrentLinkedQueue<EventIterator> iterators;

    public EventIteratorFactory(ArrowSegment segment){
        this.segment = segment;
        this.iterators = new ConcurrentLinkedQueue<>();
    }

    public EventIterator newIterator(){
        EventIterator iterator = new EventIterator(this.segment){
            @Override
            public void close() throws IOException {
                super.close();
                iterators.remove(this);
            }
        };
        iterators.add(iterator);
        return iterator;
    }

    public void swapSource(ArrowSegment segment){
        log.info("Reloading {} with {}",this.segment,segment);
        this.segment = segment;
        iterators.forEach(it -> it.reload(segment));
    }

    long first(){
        return segment.first();
    }

    long last(){
        return segment.last();
    }

    @Override
    public int compareTo(EventIteratorFactory o) {
        return Long.compare(this.first(),o.first());
    }
}
