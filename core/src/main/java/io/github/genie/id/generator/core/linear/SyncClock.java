package io.github.genie.id.generator.core.linear;

public interface SyncClock {

    int id();

    long now();

    long expiry();

}
