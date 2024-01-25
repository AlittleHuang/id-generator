package io.github.genie.id.generator.core.support;

public interface Repository {

    Locked getLockId();

    long getTimeOffset();

    long getTime();

}
