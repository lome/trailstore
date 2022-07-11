package org.lome.trailstore.utils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

//@Slf4j
//Lamport Clock implementation: https://martinfowler.com/articles/patterns-of-distributed-systems/lamport-clock.html
public class Sequencer {

    final AtomicLong latestTick = new AtomicLong(next(System.currentTimeMillis(),1));
    public final static Sequencer SHARED = new Sequencer();

    public Sequencer(long starting){
        this.latestTick.set(next(System.currentTimeMillis(),1));
    }

    public Sequencer(){
    }

    public long tick(long requestTimestamp) {
        return latestTick.getAndUpdate(current -> {
            return next(requestTimestamp,tickPart(current)+1);
        });
    }

    public long fixedTick(long timestamp){
        return next(timestamp,0);
    }

    private int epoch(long timestamp){
        return (int)Math.round(timestamp/1000.0);
    }

    //long l = (((long)x) << 32) | (y & 0xffffffffL);
    //int x = (int)(l >> 32);
    //int y = (int)l;

    private int epochPart(long tick){
        return (int)(tick >> 32);
    }

    private int tickPart(long tick){
        return (int)(tick);
    }

    private long next(int epoch, int tick){
        return (((long)epoch) << 32) | (tick & 0xffffffffL);
    }

    private long next(long timestamp, int tick){
        return (((long)epoch(timestamp)) << 32) | (tick & 0xffffffffL);
    }

    public long tick(){
        return tick(System.currentTimeMillis());
    }

    public static void main(String[] args) {
        Sequencer seq = new Sequencer();
        long now = System.currentTimeMillis();

        IntStream.range(0,100)
                .forEach(i -> System.out.println(seq.tick(System.currentTimeMillis())));
    }

}
