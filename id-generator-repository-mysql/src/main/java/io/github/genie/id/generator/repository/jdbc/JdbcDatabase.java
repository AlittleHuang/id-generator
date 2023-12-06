package io.github.genie.id.generator.repository.jdbc;


import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcDatabase {

    void executeTableDDL(Connection connection) throws SQLException;

    Locker queryExpiredForUpdate(Connection connection) throws SQLException;

    int queryNextId(Connection connection) throws SQLException;

    int lock(Connection connection, int lockId, String oldKey, String newKey, int expirySeconds) throws SQLException;

    boolean newLock(Connection connection, int id, int expirySeconds, String key) throws SQLException;

    long queryAwaitTime(Connection connection, int maxId) throws SQLException;

    long queryTimeOffsetConfig(Connection connection);

    long queryTime(Connection connection) throws SQLException;

    default void doInTransaction(Connection connection, ConnectionConsumer consumer) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        if (autoCommit) {
            connection.setAutoCommit(false);
        }
        try {
            consumer.doInConnection(connection);
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            if (autoCommit) {
                connection.setAutoCommit(true);
            }
        }
    }

    class Locker {
        private final int id;

        private final String key;

        public Locker(int id, String key) {
            this.id = id;
            this.key = key;
        }

        public int getId() {
            return id;
        }

        public String getKey() {
            return key;
        }
    }

}
