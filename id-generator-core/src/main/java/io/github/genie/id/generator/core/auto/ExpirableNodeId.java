package io.github.genie.id.generator.core.auto;

public interface ExpirableNodeId {
    int id();

    long expiry();
}
