package io.github.genie.id.generator.repository.jdbc;

import io.github.genie.id.generator.repository.jdbc.core.log.Log;

public class LogTest {

    public static void main(String[] args) {
        Log log = Log.get(Log.class);
        log.debug(() -> "debug");
    }

}
