package io.github.genie.id.generator.core.support;

public interface Clock {
    Clock DEFAULT = System::currentTimeMillis;
    long now();
}
