package io.github.genie.core.support;

public interface Repository {

    int getLockId();

    long getTimeOffset();

    void initTable(int bitWidth);

}
