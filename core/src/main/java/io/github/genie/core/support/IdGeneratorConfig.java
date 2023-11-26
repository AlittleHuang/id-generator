package io.github.genie.core.support;

public class IdGeneratorConfig {

    public static final int DEFAULT_SERIAL_WIDTH = 10;
    public static final int DEFAULT_DEVICE_ID_WIDTH = 12;
    public static final int DEFAULT_TIME_DISCARD_WIDTH = 2;

    private final int serialSize;
    private final long serialMax;
    private final long serialMask;

    private final int deviceIdSize;
    private final int deviceIdMax;
    private final int timeDiscardWidth;

    public IdGeneratorConfig() {
        this(DEFAULT_SERIAL_WIDTH, DEFAULT_DEVICE_ID_WIDTH, DEFAULT_TIME_DISCARD_WIDTH);
    }

    public IdGeneratorConfig(int serialSize, int deviceIdSize, int timeDiscardWidth) {
        this.serialSize = serialSize;
        this.serialMax = 1L << serialSize;
        this.timeDiscardWidth = timeDiscardWidth;
        this.serialMask = serialMax - 1;

        this.deviceIdSize = deviceIdSize;
        this.deviceIdMax = 1 << deviceIdSize;
    }

    public int getSerialSize() {
        return serialSize;
    }

    public long getSerialMax() {
        return serialMax;
    }

    public long getSerialMask() {
        return serialMask;
    }

    public int getDeviceIdSize() {
        return deviceIdSize;
    }

    public int getDeviceIdMax() {
        return deviceIdMax;
    }

    public int getTimeDiscardWidth() {
        return timeDiscardWidth;
    }

}
