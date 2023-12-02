package io.github.genie.id.generator.repository.jdbc.core;

public interface IdGeneratorFactory {

    IdGenerator getIdGenerator(String key);


}
