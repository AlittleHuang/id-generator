package io.github.genie.id.generator.core.support;

public interface MillisClock {
    MillisClock DEFAULT = System::currentTimeMillis;
    long now();
}
