package org.lome.trailstore.storage.segment;

public interface EventAccessor {
    long getId();
    byte[] getKey();
    byte[] getMetadata();
    byte[] getData();
}
