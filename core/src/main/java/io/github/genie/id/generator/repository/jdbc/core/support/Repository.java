package io.github.genie.id.generator.repository.jdbc.core.support;

public interface Repository {

    int getLockId();

    long getTimeOffset();

}
