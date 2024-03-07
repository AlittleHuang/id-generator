package io.github.genie.id.generator.core.support;

import io.github.genie.id.generator.core.IdGenerator;

public class DistributedIdGenerator implements IdGenerator {
    public static final int DEFAULT_MACHINE_BITS = 10;
    public static final int DEFAULT_SEQUENCE_BITS = 12;

    private final LocalIdGenerator localIdGenerator;
    private final int nodeIdBits;
    private final int nodeId;

    public DistributedIdGenerator(int nodeId, long startStamp) {
        this(nodeId, startStamp, DEFAULT_SEQUENCE_BITS, DEFAULT_MACHINE_BITS, Clock.DEFAULT);
    }

    public DistributedIdGenerator(int nodeId, long startStamp, int sequenceBits, int nodeIdBits, Clock clock) {
        this(new LocalIdGenerator(sequenceBits, startStamp, clock), nodeIdBits, nodeId);
    }

    public DistributedIdGenerator(LocalIdGenerator localIdGenerator, int nodeIdBits, int nodeId) {
        this.localIdGenerator = localIdGenerator;
        this.nodeIdBits = nodeIdBits;
        this.nodeId = nodeId;
    }

    @Override
    public long nextId() {
        return localIdGenerator.nextId() << nodeIdBits | nodeId;
    }

    public int getNodeIdBits() {
        return nodeIdBits;
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getTime(long id) {
        return localIdGenerator.getTime(id >> nodeIdBits);
    }


}
