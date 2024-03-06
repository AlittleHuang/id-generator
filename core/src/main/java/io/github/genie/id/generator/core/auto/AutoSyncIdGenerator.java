package io.github.genie.id.generator.core.auto;

import io.github.genie.id.generator.core.IdGenerator;
import io.github.genie.id.generator.core.auto.SyncClock.Clock;
import io.github.genie.id.generator.core.support.SnowFlakeIdGenerator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AutoSyncIdGenerator implements IdGenerator {
    private final Lock lock = new ReentrantLock();
    private volatile SnowFlakeIdGenerator generator;
    private final SyncClock syncClock;
    private final int sequenceBits;
    private final int machineBits;

    public AutoSyncIdGenerator(SyncClock syncClock, Config config) {
        this.syncClock = syncClock;
        this.sequenceBits = config.getSerialBits();
        this.machineBits = config.getIdBits();
    }

    @Override
    public long nextId() {
        Clock clock = syncClock.acquire();
        long id = getIdGenerator(clock).nextId();
        if (generator.getTime(id) > clock.expiry()) {
            throw new IllegalStateException("expired");
        }
        return id;
    }

    protected SnowFlakeIdGenerator getIdGenerator(Clock clock) {
        if (isGeneratorExpired(clock)) {
            lock.lock();
            try {
                if (isGeneratorExpired(clock)) {
                    generator = new SnowFlakeIdGenerator(
                            machineBits,
                            clock.id(),
                            sequenceBits,
                            clock.startStamp(),
                            clock
                    );
                }
            } finally {
                lock.unlock();
            }
        }
        return generator;
    }

    private boolean isGeneratorExpired(Clock clock) {
        return generator == null || clock.id() != generator.getMachine();
    }
}
