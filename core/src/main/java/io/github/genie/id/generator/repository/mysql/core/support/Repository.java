package io.github.genie.id.generator.repository.mysql.core.support;

public interface Repository {

    int getLockId();

    long getTimeOffset();

    void initTable(int bitWidth);

}
