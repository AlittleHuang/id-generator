package io.github.genie.id.generator.core.linear;

import io.github.genie.id.generator.core.IdGenerator;
import io.github.genie.id.generator.core.IdGeneratorFactory;

public class LinearIdGeneratorFactory implements IdGeneratorFactory {

    private final Config config;
    private final SyncClock clock;

    public LinearIdGeneratorFactory(Config config, SyncClock clock) {
        this.config = config;
        this.clock = clock;
    }

    @Override
    public IdGenerator getIdGenerator(String key) {
        return new LinearTimeIdGenerator(clock, config);
    }
}
