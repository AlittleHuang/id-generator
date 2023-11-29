package io.github.genie.id.generator.repository.mysql;

import io.github.genie.id.generator.repository.mysql.core.log.Log;
import io.github.genie.id.generator.repository.mysql.core.support.IdGeneratorConfig;
import io.github.genie.id.generator.repository.mysql.core.support.Repository;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MySqlRepository implements Repository, AutoCloseable {

    private final Log log = Log.get(MySqlRepository.class);

    public static final int UNINITIALIZED_ID = -1;


    private final String LOCK_KEY = randomKey();

    public static final String[] CREATE_TABLE_DDL =
            {
                    "create table if not exists `id_gen_server_lock` (" +
                    "`id` int not null," +
                    "`expiry_time` datetime not null," +
                    "`lock_key` varchar(25) not null," +
                    "primary key (`id`)) " +
                    "engine=innodb default charset=ascii collate=ascii_bin",

                    "create table if not exists `id_gen_config` (" +
                    "`id` varchar(64) not null," +
                    "`config` varchar(255) not null," +
                    "primary key (`id`)) " +
                    "engine=innodb default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;"
            };

    private final static String EXISTS_QUERY_SQL = "select id from id_gen_server_lock limit 1";

    private final static String ID_QUERY_SQL =
            "select id from id_gen_server_lock where id=(select min(id) " +
            "from id_gen_server_lock where now()>expiry_time) for update";

    private final static String LOCK_SQL =
            "update id_gen_server_lock " +
            "set expiry_time=date_add(now(),interval ? second), lock_key=? where id=?";

    private final static String MIN_WAIT_TIME_SQL =
            "select timestampdiff(microsecond,now(),min(expiry_time))/1000 as wait_time from id_gen_server_lock";

    private final static String QUERY_TIME_OFFSET_SQL =
            "select config from id_gen_config where id='time_offset'";

    private final static String CREATE_TIME_OFFSET_SQL =
            "insert into id_gen_config (id,config) values ('time_offset',ROUND(unix_timestamp(now())*1000))";

    private static final String TIME_QUERY = "select unix_timestamp(now(3))*1000";

    private final ConnectionProvider connectionProvider;
    private final ScheduledFuture<?> scheduled;
    private final long timeOffset;
    private final int lockExpirySeconds;
    private final ScheduledExecutorService scheduledExecutorService;
    private boolean shutdownServiceOnClose;

    private volatile int id = UNINITIALIZED_ID;
    private Long difTime;

    private String renewSql;

    public MySqlRepository(ConnectionProvider connectionProvider) {
        this(
                Executors.newSingleThreadScheduledExecutor(),
                connectionProvider,
                Duration.ofSeconds(20),
                Duration.ofSeconds(5)
        );
        shutdownServiceOnClose = true;
    }

    private MySqlRepository(ScheduledExecutorService scheduledExecutorService,
                            ConnectionProvider connectionProvider,
                            Duration lockExpiryDuration,
                            Duration lockRenewalPeriod) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.connectionProvider = connectionProvider;
        this.lockExpirySeconds = (int) lockExpiryDuration.getSeconds();
        proofreadingTime();
        executeDDL();
        initTable(IdGeneratorConfig.DEFAULT_DEVICE_ID_WIDTH);
        acquireId();
        this.timeOffset = initTimeOffset();
        this.scheduled = initScheduled(scheduledExecutorService, lockRenewalPeriod);
    }

    private ScheduledFuture<?> initScheduled(ScheduledExecutorService service, Duration lockRenewalPeriod) {
        long period = lockRenewalPeriod.toMillis();
        return service.scheduleAtFixedRate(this::keepLock, period, period, TimeUnit.MILLISECONDS);
    }

    private void keepLock() {
        if (id != UNINITIALIZED_ID) {
            try {
                renewExpiration();
            } catch (Exception e) {
                log.error("renew expiration failed", e);
            }
        }
        if (id == UNINITIALIZED_ID) {
            try {
                acquireId();
            } catch (Exception e) {
                log.error("acquire id failed", e);
            }
        }
        try {
            proofreadingTime();
        } catch (Exception e) {
            log.error("acquire id failed", e);
        }
    }

    @Override
    public int getLockId() {
        if (id == UNINITIALIZED_ID) {
            throw new IllegalStateException("id uninitialized");
        }
        return id;
    }

    private void renewExpiration() {
        long start = System.currentTimeMillis();
        doInConnection(connection -> {
            Statement renew = connection.createStatement();
            int updateRoles = renew.executeUpdate(renewSql);
            log.trace(() -> renewSql);
            if (updateRoles == 1) {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } else if (updateRoles == 0) {
                updateId(UNINITIALIZED_ID);
            } else {
                updateId(UNINITIALIZED_ID);
                throw new IllegalStateException();
            }
        });
        log.trace(() -> "renew in " + (System.currentTimeMillis() - start) + "ms");
        log.trace(() -> "renew success: " + (id != UNINITIALIZED_ID));
    }

    private void acquireId() {
        while (id == UNINITIALIZED_ID) {
            try {
                long waitTime = acquireId(LOCK_KEY);
                if (waitTime > 0) {
                    synchronized (this) {
                        try {
                            wait(waitTime);
                        } catch (InterruptedException ignore) {
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeSqlException(e);
            }
        }
    }

    private long acquireId(String deviceId) throws SQLException {
        AtomicLong result = new AtomicLong();
        doInTransaction(connection -> {
            ResultSet idResult = connection.createStatement().executeQuery(ID_QUERY_SQL);
            if (idResult.next()) {
                updateId(idResult.getInt(1));
                PreparedStatement statement = connection.prepareStatement(LOCK_SQL);
                statement.setInt(1, lockExpirySeconds);
                statement.setString(2, deviceId);
                statement.setInt(3, id);
                statement.execute();
            } else {
                ResultSet waitTimeResult = connection.createStatement().executeQuery(MIN_WAIT_TIME_SQL);
                if (waitTimeResult.next()) {
                    long waitTime = waitTimeResult.getLong(1);
                    result.set(waitTime);
                } else {
                    throw new IllegalStateException("database not initialized");
                }
            }
        });

        return result.get();
    }

    @Override
    public long getTimeOffset() {
        return timeOffset + difTime;
    }

    private long initTimeOffset() {
        AtomicLong result = new AtomicLong();
        doInConnection(connection -> result.set(getTimeOffsetInConnection(connection)));
        return result.get();
    }

    private void proofreadingTime() {
        doInConnection(connection -> {
            long start = System.currentTimeMillis();
            ResultSet resultSet = connection.prepareStatement(TIME_QUERY).executeQuery();
            if (resultSet.next()) {
                long now = System.currentTimeMillis();
                long dbTime = resultSet.getLong(1);
                long dif = (now + start) / 2 - dbTime;
                log.trace(() -> "time dif: " + dif + "ms");
                if (difTime == null || difTime < dif) {
                    difTime = dif;
                    log.debug(() -> "update time dif: " + dif + "ms");
                }
            }
        });
    }

    private long getTimeOffsetInConnection(Connection conn) {
        try {
            long now = System.currentTimeMillis();
            ResultSet resultSet = conn.createStatement().executeQuery(QUERY_TIME_OFFSET_SQL);
            log.debug(() -> "get time_offset in " + (System.currentTimeMillis() - now) + "ms");
            if (resultSet.next()) {
                long offset = resultSet.getLong(1);
                log.debug(() -> "offset: " + offset);
                return offset;
            } else {
                doInTransaction(conn, connection -> connection.createStatement().executeUpdate(CREATE_TIME_OFFSET_SQL));
                return getTimeOffset();
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    @Override
    public void initTable(int bitWidth) {
        try {
            doInitTable(bitWidth);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    private void updateId(int id) {
        this.id = id;
        if (id >= 0) {
            renewSql = "update id_gen_server_lock set" +
                       " expiry_time=date_add(now(),interval " + lockExpirySeconds + " second) " +
                       "where" +
                       " id=" + id + " and" +
                       " lock_key='" + LOCK_KEY + "' and" +
                       " now() < expiry_time";
        } else {
            renewSql = null;
        }
    }

    private void executeDDL() {
        doInConnection(connection -> {
            for (String ddl : CREATE_TABLE_DDL) {
                connection.createStatement().execute(ddl);
            }
        });
    }

    private void doInitTable(int bitWidth) throws SQLException {
        doInTransaction(connection -> {
            connection.commit();
            ResultSet existsResult = connection.createStatement().executeQuery(EXISTS_QUERY_SQL);
            if (!existsResult.next()) {
                int size = (1 << bitWidth) - 1;
                if (size > 0) {
                    StringBuilder sb = new StringBuilder(
                            "INSERT INTO `id_gen_server_lock` (`id`, `expiry_time`, `lock_key`) VALUES (0,NOW(3), '')");
                    for (int i = 1; i < size; i++) {
                        sb.append(",(").append(i).append(",NOW(3), '')");
                    }
                    connection.createStatement().executeUpdate(sb.toString());
                }
            }
        });
    }

    private void doInConnection(ConnectionConsumer connectionConsumer) {
        try (Connection connection = connectionProvider.getConnection()) {
            connectionConsumer.doInConnection(connection);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    private void doInTransaction(ConnectionConsumer connectionConsumer) {
        doInConnection(connection -> doInTransaction(connection, connectionConsumer));
    }


    private void doInTransaction(Connection connection, ConnectionConsumer connectionConsumer) {
        try {
            RuntimeException exception = null;
            boolean autoCommit = connection.getAutoCommit();
            if (autoCommit) {
                connection.setAutoCommit(false);
            }

            try {
                connectionConsumer.doInConnection(connection);
            } catch (SQLException e) {
                exception = new RuntimeSqlException(e);
            } catch (RuntimeException e) {
                exception = e;
            }
            if (exception == null) {
                connection.commit();
            } else {
                connection.rollback();
            }
            if (autoCommit) {
                connection.setAutoCommit(true);
            }
            if (exception != null) {
                throw exception;
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }


    private static String randomKey() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = ByteBuffer.allocate(Long.BYTES * 2)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        return new BigInteger(1, bytes).toString(Character.MAX_RADIX);
    }

    @Override
    public void close() {
        scheduled.cancel(true);
        if (shutdownServiceOnClose) {
            scheduledExecutorService.shutdown();
        }
    }

    interface ConnectionConsumer {
        void doInConnection(Connection connection) throws SQLException;
    }

}