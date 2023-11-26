package io.github.genie.repository;

import java.sql.SQLException;

public class RuntimeSqlException extends RuntimeException {
    public RuntimeSqlException(SQLException cause) {
        super(cause);
    }
}
