package org.lome.trailstore.storage.chunks;

public interface EventAccessor{
    long getId();
    byte[] getKey();
    byte[] getMetadata();
    byte[] getData();
}
