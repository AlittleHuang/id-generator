package io.github.genie.id.generator.core.linear;

public class Config {

    public static final int DEFAULT_SERIAL_BITS = 10;
    public static final int DEFAULT_ID_BITS = 12;
    public static final int DEFAULT_TIME_DISCARD_BITS = 2;

    private final int serialBits;
    private final int idBits;
    private final int timeDiscardBits;

    public Config() {
        this(DEFAULT_SERIAL_BITS, DEFAULT_ID_BITS, DEFAULT_TIME_DISCARD_BITS);
    }

    public Config(int serialBits, int idBits, int timeDiscardBits) {
        this.serialBits = serialBits;
        this.idBits = idBits;
        this.timeDiscardBits = timeDiscardBits;
    }

    public int getSerialBits() {
        return serialBits;
    }

    public int getIdBits() {
        return idBits;
    }

    public int getTimeDiscardBits() {
        return timeDiscardBits;
    }
}
