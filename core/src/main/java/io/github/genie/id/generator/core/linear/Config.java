package io.github.genie.id.generator.core.linear;

public class Config {

    public static final int DEFAULT_SERIAL_BITS = 12;
    public static final int DEFAULT_ID_BITS = 10;
    private final int serialBits;
    private final int idBits;

    public Config() {
        this(DEFAULT_SERIAL_BITS, DEFAULT_ID_BITS);
    }

    public Config(int serialBits, int idBits) {
        this.serialBits = serialBits;
        this.idBits = idBits;
    }

    public int getSerialBits() {
        return serialBits;
    }

    public int getIdBits() {
        return idBits;
    }
}
