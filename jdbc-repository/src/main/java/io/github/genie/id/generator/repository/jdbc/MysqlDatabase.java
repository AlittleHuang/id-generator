package io.github.genie.id.generator.repository.jdbc;

import java.sql.*;

public class MysqlDatabase implements JdbcDatabase {

    @Override
    public void executeTableDDL(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table if not exists `id_generator_lock` (" +
                              "`id` int not null," +
                              "`expiry_time` datetime not null," +
                              "`lock_key` char(25) not null," +
                              "primary key (`id`)) " +
                              "engine=innodb default charset=ascii collate=ascii_bin");

            statement.execute("create table if not exists `id_generator_config` (" +
                              "`id` varchar(64) not null," +
                              "`config` varchar(255) not null," +
                              "primary key (`id`)) " +
                              "engine=innodb default charset=utf8mb4 collate=utf8mb4_0900_ai_ci");
        }
    }

    @Override
    public Locker queryExpiredForUpdate(Connection connection) throws SQLException {
        String sql = "select id,lock_key from id_generator_lock where id=(select min(id) " +
                     "from id_generator_lock where now()>expiry_time) for update";
        try (Statement statement = connection.createStatement();
             ResultSet idResult = statement.executeQuery(sql)) {
            if (idResult.next()) {
                return new Locker(
                        idResult.getInt(1),
                        idResult.getString(2)
                );
            }
        }
        return null;
    }

    @Override
    public int queryNextId(Connection connection) throws SQLException {
        String sql = "select ifNull(min(l.id+1),0) as id from id_generator_lock l " +
                     "left join id_generator_lock r on l.id = r.id-1 where r.id is null";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public int lock(Connection connection, int lockId, String oldKey, String newKey, int expirySeconds) throws SQLException {
        String sql = "update id_generator_lock " +
                     "set expiry_time=date_add(now(),interval ? second),lock_key=? " +
                     "where id=? and lock_key=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, expirySeconds);
            statement.setString(2, newKey);
            statement.setInt(3, lockId);
            statement.setString(4, oldKey);
            return statement.executeUpdate();
        }
    }

    @Override
    public boolean newLock(Connection connection, int id, int expirySeconds, String key) throws SQLException {
        String sql = "insert ignore into `id_generator_lock` (`id`,`expiry_time`,`lock_key`) " +
                     "values (?,date_add(now(),interval ? second),?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, id);
            statement.setInt(2, expirySeconds);
            statement.setString(3, key);
            int updateRoles = statement.executeUpdate();
            return updateRoles == 1;
        }
    }

    @Override
    public long queryAwaitTime(Connection connection, int maxId) throws SQLException {
        String sql = "select timestampdiff(microsecond,now(),min(expiry_time))/1000 as wait_time " +
                     "from id_generator_lock where id between 0 and ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, maxId);
            try (ResultSet waitTimeResult = statement.executeQuery()) {
                if (waitTimeResult.next()) {
                    return waitTimeResult.getLong(1);
                } else {
                    throw new IllegalStateException("database not initialized");
                }
            }
        }
    }

    @Override
    public long queryTimeOffsetConfig(Connection connection) {
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
                    return queryTimeOffsetConfig(connection);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    @Override
    public long queryTime(Connection connection) throws SQLException {
        String sql = "select unix_timestamp(now(3))*1000";
        try (ResultSet resultSet = connection.prepareStatement(sql).executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        }
        throw new IllegalStateException();
    }

}
