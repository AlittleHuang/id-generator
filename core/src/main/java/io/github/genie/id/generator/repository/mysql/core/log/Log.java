package io.github.genie.id.generator.repository.mysql.core.log;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Log {

    static {
        init();
    }

    public static void init() {
        ClassLoader cl = Log.class.getClassLoader();
        InputStream inputStream;
        if (cl != null) {
            inputStream = cl.getResourceAsStream("logging.properties");
        } else {
            inputStream = ClassLoader
                    .getSystemResourceAsStream("logging.properties");
        }
        try {
            LogManager.getLogManager().readConfiguration(inputStream);
        } catch (SecurityException | IOException e) {
            get(Log.class).error("load file failed", e);
        }
    }

    private final Logger logger;

    public Log(Logger logger) {
        this.logger = logger;
    }

    public void error(String message, Exception e) {
        logger.log(Level.SEVERE, message, e);
    }

    public void debug(Supplier<String> messageSupplier) {
        logger.log(Level.FINE, messageSupplier);
    }


    public void trace(Supplier<String> messageSupplier) {
        logger.log(Level.FINER, messageSupplier);
    }

    public static Log get(Class<?> type) {
        return get(type.getName());
    }

    public static Log get(String name) {
        Logger logger = Logger.getLogger(name);
        return new Log(logger);
    }

}
