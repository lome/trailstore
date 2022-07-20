package org.lome.trailstore.storage.segment;

public interface EventFilter{
    boolean filter(EventAccessor eventAccessor);

}
