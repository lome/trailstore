package org.lome.trailstore.storage.chunks;

public interface EventFilter{
    boolean filter(EventAccessor eventAccessor);

}
