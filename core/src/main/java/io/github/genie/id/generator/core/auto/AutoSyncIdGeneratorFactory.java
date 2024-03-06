package io.github.genie.id.generator.core.auto;

import io.github.genie.id.generator.core.IdGenerator;
import io.github.genie.id.generator.core.IdGeneratorFactory;

public class AutoSyncIdGeneratorFactory implements IdGeneratorFactory {

    private final Config config;
    private final SyncClock clock;

    public AutoSyncIdGeneratorFactory(Config config, SyncClock clock) {
        this.config = config;
        this.clock = clock;
    }

    @Override
    public IdGenerator getIdGenerator(String key) {
        return new AutoSyncIdGenerator(clock, config);
    }
}
