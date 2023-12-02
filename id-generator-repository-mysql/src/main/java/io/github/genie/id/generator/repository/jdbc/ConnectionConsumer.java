package io.github.genie.id.generator.repository.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionConsumer {
    void doInConnection(Connection connection) throws SQLException;
}
