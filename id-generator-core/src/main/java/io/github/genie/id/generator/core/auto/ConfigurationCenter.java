package io.github.genie.id.generator.core.auto;

import io.github.genie.id.generator.core.support.Clock;

public interface ConfigurationCenter {

    Clock clock();

    ExpirableNodeId acquireNodeId();

    int machineBits();

    int sequenceBits();

    long startStamp();


}
