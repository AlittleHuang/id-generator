package io.github.genie.id.generator.core.support;

import io.github.genie.id.generator.core.IdGenerator;

import java.util.concurrent.atomic.AtomicLong;

public class LocalIdGenerator implements IdGenerator {
    private final AtomicLong generator = new AtomicLong();
    private final int sequenceBits;
    private final long startStamp;
    private final MillisClock clock;

    public LocalIdGenerator(int sequenceBits, long startStamp, MillisClock clock) {
        this.sequenceBits = sequenceBits;
        this.startStamp = startStamp;
        this.clock = clock;
    }

    @Override
    public long nextId() {
        return generator.updateAndGet(this::computeNext);
    }

    private long computeNext(long origin) {
        long now = clock.now();
        return getTime(origin) >= now ? 1 + origin : (now - startStamp) << sequenceBits;
    }

    public int getSequenceBits() {
        return sequenceBits;
    }

    public long getStartStamp() {
        return startStamp;
    }

    public MillisClock getClock() {
        return clock;
    }

    public long getTime(long id) {
        return (id >> sequenceBits) + startStamp;
    }
}
