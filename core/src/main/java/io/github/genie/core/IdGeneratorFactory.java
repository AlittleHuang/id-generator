package io.github.genie.core;

public interface IdGeneratorFactory {

    default long nextId(String key) {
        return getIdGenerator(key).nextId();
    }

    IdGenerator getIdGenerator(String key);


}
