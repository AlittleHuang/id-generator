package io.github.genie.id.generator.core.auto;

public class InitialConfiguration {

    public static final int DEFAULT_SEQUENCE_BITS = 12;
    public static final int DEFAULT_ID_BITS = 10;
    private final int sequenceBits;
    private final int nodeIdBits;

    public InitialConfiguration() {
        this(DEFAULT_SEQUENCE_BITS, DEFAULT_ID_BITS);
    }

    public InitialConfiguration(int sequenceBits, int nodeIdBits) {
        this.sequenceBits = sequenceBits;
        this.nodeIdBits = nodeIdBits;
    }

    public int getSequenceBits() {
        return sequenceBits;
    }

    public int getNodeIdBits() {
        return nodeIdBits;
    }
}
