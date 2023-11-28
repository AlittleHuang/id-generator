package io.github.genie.id.generator.repository.mysql;

import java.sql.SQLException;

public class RuntimeSqlException extends RuntimeException {
    public RuntimeSqlException(SQLException cause) {
        super(cause);
    }
}
