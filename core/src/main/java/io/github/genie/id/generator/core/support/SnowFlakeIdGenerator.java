package io.github.genie.id.generator.core.support;

import io.github.genie.id.generator.core.IdGenerator;

public class SnowFlakeIdGenerator implements IdGenerator {
    private final LocalIdGenerator localIdGenerator;
    private final int machineBits;
    private final int machine;

    public SnowFlakeIdGenerator(int machineBits, int machine, int sequenceBits, long startStamp, MillisClock clock) {
        this(new LocalIdGenerator(sequenceBits, startStamp, clock), machineBits, machine);
    }

    public SnowFlakeIdGenerator(LocalIdGenerator localIdGenerator, int machineBits, int machine) {
        this.localIdGenerator = localIdGenerator;
        this.machineBits = machineBits;
        this.machine = machine;
    }

    @Override
    public long nextId() {
        return localIdGenerator.nextId() << machineBits | machine;
    }

    public int getMachineBits() {
        return machineBits;
    }

    public int getMachine() {
        return machine;
    }

    public long getTime(long id) {
        return localIdGenerator.getTime(id >> machineBits);
    }


}
