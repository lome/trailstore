package org.lome.trailstore.storage.chunks;

import lombok.*;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ChunkInfo {
    int elements;
    long first;
    long last;
}
