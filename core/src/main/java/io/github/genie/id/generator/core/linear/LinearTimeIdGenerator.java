package io.github.genie.id.generator.core.linear;

import io.github.genie.id.generator.core.IdGenerator;

import java.util.concurrent.atomic.AtomicLong;

public class LinearTimeIdGenerator implements IdGenerator {

    private final AtomicLong time_serial = new AtomicLong();

    private final SyncClock syncClock;
    private final int serialBits;
    private final int idBits;
    private final int timeDiscardBits;

    public LinearTimeIdGenerator(SyncClock syncClock, Config config) {
        this.syncClock = syncClock;
        this.serialBits = config.getSerialBits();
        this.idBits = config.getIdBits();
        this.timeDiscardBits = config.getTimeDiscardBits();
    }

    @Override
    public long nextId() {
        long incremented = increment(syncClock);
        return incremented << idBits | syncClock.id();
    }

    public long increment(SyncClock clock) {
        return time_serial.updateAndGet(origin -> {
            long next = computeNext(clock, origin);
            return requireNotExpired(next, clock.expiry());
        });
    }

    private long computeNext(SyncClock clock, long origin) {
        long time = origin >> serialBits;
        long now = clock.now() >> timeDiscardBits;
        return time >= now ? 1 + origin : now << serialBits;
    }

    private long requireNotExpired(long next, long expiry) {
        long nextTime = next >> serialBits;
        long expiryTime = expiry >> timeDiscardBits;
        if (nextTime > expiryTime) {
            throw new IllegalStateException();
        }
        return next;
    }


}
