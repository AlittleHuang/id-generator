package io.github.genie.id.generator.repository.jdbc.core.support;

import io.github.genie.id.generator.repository.jdbc.core.IdGenerator;
import io.github.genie.id.generator.repository.jdbc.core.IdGeneratorFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultIdGeneratorFactory implements IdGeneratorFactory {

    private final Map<String, IdGenerator> generators = new ConcurrentHashMap<>();
    private final IdGeneratorConfig config;
    private final Repository repository;

    public DefaultIdGeneratorFactory(Repository repository) {
        this(new IdGeneratorConfig(), repository);
    }

    public DefaultIdGeneratorFactory(IdGeneratorConfig config, Repository repository) {
        this.config = config;
        this.repository = repository;
    }

    @Override
    public IdGenerator getIdGenerator(String key) {
        Objects.requireNonNull(key);
        return generators.computeIfAbsent(key, k -> new DefaultIdGenerator(config, repository));
    }

}
