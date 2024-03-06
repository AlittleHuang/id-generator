package io.github.genie.id.generator.repository.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class MysqlSyncClock extends JdbcSyncClock {
    public MysqlSyncClock(int maxId, ConnectionProvider connectionProvider) {
        super(maxId, connectionProvider);
    }

    public MysqlSyncClock(int maxId,
                          String key,
                          ConnectionProvider connectionProvider,
                          int expirySeconds,
                          Duration lockRenewalPeriod,
                          ScheduledExecutorService scheduledExecutorService) {
        super(maxId, key, connectionProvider, expirySeconds, lockRenewalPeriod, scheduledExecutorService);
    }

    public long getAwaitTime(Connection connection, int maxId) throws SQLException {
        String sql = "select timestampdiff(microsecond,now(),min(expiry_time))/1000 as wait_time " +
                     "from id_generator_lock where id between 0 and " + maxId;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet waitTimeResult = statement.executeQuery(sql)) {
                if (waitTimeResult.next()) {
                    return waitTimeResult.getLong(1);
                } else {
                    throw new IllegalStateException("database not initialized");
                }
            }
        }
    }

    protected boolean insertRecord(Connection connection, int id, String key, int expirySeconds) throws SQLException {
        String sql = "insert ignore into `id_generator_lock` (`id`,`expiry_time`,`lock_key`) " +
                     "values (?,date_add(now(),interval ? second),?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, id);
            statement.setInt(2, expirySeconds);
            statement.setString(3, key);
            int updateRows = statement.executeUpdate();
            return updateRows == 1;
        }

    }

    protected boolean renewTtl(Connection connection, int id, String oldKey, String newKey, int expirySeconds) throws SQLException {
        String sql = "update id_generator_lock " +
                     "set expiry_time=date_add(now(),interval ? second),lock_key=? " +
                     "where id=? and lock_key=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, expirySeconds);
            statement.setString(2, newKey);
            statement.setInt(3, id);
            statement.setString(4, oldKey);
            return statement.executeUpdate() == 1;
        }
    }

    protected Integer getNextId(Connection connection) throws SQLException {
        String sql = "select ifNull(min(l.id+1),0) as id from id_generator_lock l " +
                     "left join id_generator_lock r on l.id = r.id-1 where r.id is null and l.id<" + maxId;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                return null;
            }
        }
    }


    protected Record getExpiredRecord(Connection connection, int maxId) throws SQLException {
        String sql = "select id,lock_key from id_generator_lock where id=(select min(id) " +
                     "from id_generator_lock where now()>expiry_time and id>=0 and id<=" + maxId + ") for update";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return new Record(
                        resultSet.getInt(1),
                        resultSet.getString(2)
                );
            }
        }
        return null;
    }

    protected long getDbTime(Connection connection) throws SQLException {
        String sql = "select unix_timestamp(now(3))*1000";
        try (ResultSet resultSet = connection.prepareStatement(sql).executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        }
        throw new IllegalStateException();
    }

    @Override
    protected long getStartTime(Connection connection) {
        String sql = "select config from id_generator_config where id='time_offset'";
        try {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                } else {
                    String insertSql = "insert into id_generator_config (id,config) " +
                                       "values ('time_offset',round(unix_timestamp(now())*1000))";
                    statement.executeUpdate(insertSql);
                    return getStartTime(connection);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

}
