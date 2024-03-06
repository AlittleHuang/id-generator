package io.github.genie.id.generator.core.linear;

import io.github.genie.id.generator.core.support.MillisClock;

public interface SyncClock {
    Clock acquire();

    interface Clock extends MillisClock {
        int id();

        long startStamp();

        long expiry();
    }
}
