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
        int deviceId = repository.getLockId();
        if (deviceId < 0 || deviceId >= config.getDeviceIdMax()) {
            throw new IllegalStateException(
                    deviceId + ", 0 < deviceId < " + config.getDeviceIdMax()
            );
        }
        return nextNumber() << config.getDeviceIdSize() | deviceId;
    }

    public long nextNumber() {
        return generator.updateAndGet(number -> {
            long now = (System.currentTimeMillis() - repository.getTimeOffset())
                       >> config.getTimeDiscardWidth();
            long time = number >> config.getSerialSize();
            if (time >= now && (number & config.getSerialMask()) + 1 < config.getSerialMax()) {
                return number + 1;
            }
            if (time < now) {
                time = now;
            } else {
                time++;
            }
            return time << config.getSerialSize();
        });
    }
}