package io.github.genie.id.generator.repository.mysql.core;

public interface IdGeneratorFactory {

    default long nextId(String key) {
        return getIdGenerator(key).nextId();
    }

    IdGenerator getIdGenerator(String key);


}
