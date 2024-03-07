package io.github.genie.id.generator.core.auto;

import io.github.genie.id.generator.core.IdGenerator;
import io.github.genie.id.generator.core.support.DistributedIdGenerator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AutoConfigurableIdGenerator implements IdGenerator {
    private final Lock lock = new ReentrantLock();
    private volatile DistributedIdGenerator generator;
    private final ConfigurationCenter configurationCenter;

    public AutoConfigurableIdGenerator(ConfigurationCenter config) {
        this.configurationCenter = config;
    }

    @Override
    public long nextId() {
        ExpirableNodeId clock = configurationCenter.acquireNodeId();
        long id = getIdGenerator(clock).nextId();
        if (generator.getTime(id) > clock.expiry()) {
            throw new IllegalStateException("expired");
        }
        return id;
    }

    protected DistributedIdGenerator getIdGenerator(ExpirableNodeId machineId) {
        if (isGeneratorExpired(machineId)) {
            lock.lock();
            try {
                if (isGeneratorExpired(machineId)) {
                    generator = new DistributedIdGenerator(
                            machineId.id(),
                            configurationCenter.startStamp(),
                            configurationCenter.sequenceBits(),
                            configurationCenter.machineBits(),
                            configurationCenter.clock()
                    );
                }
            } finally {
                lock.unlock();
            }
        }
        return generator;
    }

    private boolean isGeneratorExpired(ExpirableNodeId node) {
        return generator == null || node.id() != generator.getNodeId();
    }
}
