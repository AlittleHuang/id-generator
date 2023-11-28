package io.github.genie.id.generator.repository.mysql;

import io.github.genie.id.generator.repository.mysql.core.log.Log;

public class LogTest {

    public static void main(String[] args) {
        Log log = Log.get(Log.class);
        log.debug(() -> "debug");
    }

}
