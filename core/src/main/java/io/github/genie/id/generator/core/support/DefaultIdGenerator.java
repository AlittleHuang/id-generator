package io.github.genie.id.generator.core.support;

import io.github.genie.id.generator.core.IdGenerator;

import java.util.concurrent.atomic.AtomicLong;

class DefaultIdGenerator implements IdGenerator {

    private final AtomicLong generator = new AtomicLong();

    private final IdGeneratorConfig config;
    private final Repository repository;

    public DefaultIdGenerator(IdGeneratorConfig config, Repository repository) {
        this.config = config;
        this.repository = repository;
    }

    @Override
    public long nextId() {
        Locked locked = repository.getLockId();
        int deviceId = locked.getId();
        if (deviceId < 0 || deviceId >= config.getDeviceIdMax()) {
            throw new IllegalStateException(
                    deviceId + ", 0 < deviceId < " + config.getDeviceIdMax()
            );
        }
        return nextNumber(locked.getUntil()) << config.getDeviceIdSize() | deviceId;
    }

    public long nextNumber(long until) {
        return generator.updateAndGet(number -> {
            long timeOffset = repository.getTimeOffset();
            long now = fixedTime(repository.getTime(), timeOffset);
            long time = number >> config.getSerialSize();
            if (time >= now && (number & config.getSerialMask()) + 1 < config.getSerialMax()) {
                return number + 1;
            }
            if (time < now) {
                time = now;
            } else {
                time++;
            }
            long maxTime = fixedTime(until, timeOffset);
            if (time > maxTime) {
                throw new IllegalStateException();
            }
            return time << config.getSerialSize();
        });
    }

    private long fixedTime(long time, long timeOffset) {
        return (time - timeOffset) >> config.getTimeDiscardWidth();
    }
}