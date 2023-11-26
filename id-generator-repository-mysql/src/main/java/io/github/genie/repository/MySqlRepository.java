package io.github.genie.repository;

import io.github.genie.core.support.Repository;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MySqlRepository implements Repository, AutoCloseable {

    private final String LOCK_KEY = randomKey();

    public static final String[] CREATE_TABLE_DDL =
            {
                    "create table if not exists `id_gen_server_lock` (" +
                    "`id` int not null," +
                    "`expiry_time` datetime not null," +
                    "`lock_key` varchar(25) not null," +
                    "primary key (`id`)," +
                    "key `idx_expiry_time` (`expiry_time`)) " +
                    "engine=innodb auto_increment=2 default charset=utf8mb4 collate=utf8mb4_0900_ai_ci",

                    "create table if not exists `id_gen_config` (" +
                    "`id` varchar(64) not null," +
                    "`config` varchar(255) not null," +
                    "primary key (`id`)) " +
                    "engine=innodb default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;"
            };

    private final static String EXISTS_QUERY_SQL = "select id from id_gen_server_lock limit 1";


    private final static String ID_QUERY_SQL =
            "select id from id_gen_server_lock where id  = (select min(id) " +
            "from id_gen_server_lock where now(3) > expiry_time) for update";

    private final static String LOCK_SQL =
            "update id_gen_server_lock " +
            "set expiry_time = date_add(now(3), interval ? second), lock_key=? where id=?";

    private final static String MIN_WAIT_TIME_SQL =
            "select timestampdiff(microsecond,now(3),min(expiry_time))/1000 as wait_time from id_gen_server_lock";

    private final static String RENEW_LOCK_SQL = "select id from id_gen_server_lock where id=? for update";

    private final static String RENEW_SQL =
            "update id_gen_server_lock set expiry_time=date_add(now(3),interval ? second) where id=? and lock_key=?";

    private final static String QUERY_TIME_OFFSET_SQL =
            "select unix_timestamp(now(3))*1000,config from id_gen_config where id='time_offset'";

    private final static String CREATE_TIME_OFFSET_SQL =
            "insert into id_gen_config (id,config) values ('time_offset',round(unix_timestamp(now(3))*1000)/1000))";


    private final ConnectionProvider connectionProvider;
    private final ScheduledFuture<?> scheduled;
    private final long timeOffset;
    private final int lockExpirySeconds;
    private final ScheduledExecutorService scheduledExecutorService;
    private int id = -1;

    public MySqlRepository(ConnectionProvider connectionProvider) {
        this(
                Executors.newSingleThreadScheduledExecutor(),
                connectionProvider,
                Duration.ofSeconds(20),
                Duration.ofSeconds(10),
                true
        );
    }

    public MySqlRepository(ScheduledExecutorService scheduledExecutorService,
                           ConnectionProvider connectionProvider,
                           Duration lockExpiryDuration,
                           Duration lockRenewalPeriod) {
        this(scheduledExecutorService,
                connectionProvider,
                lockExpiryDuration,
                lockRenewalPeriod,
                false);
    }

    private MySqlRepository(ScheduledExecutorService scheduledExecutorService,
                            ConnectionProvider connectionProvider,
                            Duration lockExpiryDuration,
                            Duration lockRenewalPeriod,
                            boolean privateService) {
        if (privateService) {
            this.scheduledExecutorService = scheduledExecutorService;
        } else {
            this.scheduledExecutorService = null;
        }
        this.connectionProvider = connectionProvider;
        this.lockExpirySeconds = (int) lockExpiryDuration.getSeconds();
        executeDDL();
        initId();
        this.timeOffset = initTimeOffset();
        this.scheduled = initScheduled(scheduledExecutorService, lockRenewalPeriod);
    }

    private ScheduledFuture<?> initScheduled(ScheduledExecutorService service, Duration lockRenewalPeriod) {
        long period = lockRenewalPeriod.toMillis();
        return service.scheduleAtFixedRate(this::renewLock, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public int getLockId() {
        if (id == -1) {
            throw new IllegalStateException("id uninitialized");
        }
        return id;
    }

    private void renewLock() {
        try {
            doInTransaction(connection -> {
                PreparedStatement lockRow = connection.prepareStatement(RENEW_LOCK_SQL);
                lockRow.setInt(1, id);
                lockRow.execute();
                PreparedStatement renew = connection.prepareStatement(RENEW_SQL);
                renew.setInt(1, lockExpirySeconds);
                renew.setInt(2, id);
                renew.setString(3, LOCK_KEY);
                int updateRoles = renew.executeUpdate();
                if (updateRoles == 0) {
                    initId();
                }
            });
        } catch (Exception e) {
            // TODO
            e.printStackTrace();
        }
    }

    private void initId() {
        while (id == -1) {
            try {
                long waitTime = queryId(LOCK_KEY);
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

    private long queryId(String deviceId) throws SQLException {
        AtomicLong result = new AtomicLong();
        doInTransaction(connection -> {
            ResultSet idResult = connection.createStatement().executeQuery(ID_QUERY_SQL);
            if (idResult.next()) {
                id = idResult.getInt(1);
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
                    throw new IllegalStateException("Database not initialized");
                }
            }
        });

        return result.get();
    }

    @Override
    public long getTimeOffset() {
        return timeOffset;
    }

    private long initTimeOffset() {
        AtomicLong result = new AtomicLong();
        doInConnection(connection -> result.set(getTimeOffsetInConnection(connection)));
        return result.get();
    }

    private long getTimeOffsetInConnection(Connection conn) {
        try {
            long now = System.currentTimeMillis();
            ResultSet resultSet = conn.createStatement().executeQuery(QUERY_TIME_OFFSET_SQL);
            System.out.println(System.currentTimeMillis() - now);
            if (resultSet.next()) {
                long dbTime = resultSet.getLong(1);
                long dif = now - dbTime;
                long offset = resultSet.getLong(2);
                System.out.println("offset: " + offset + ", dif:" + dif);
                return dif + offset;
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
            // TODO
            e.printStackTrace();
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
            } catch (SQLException t) {
                exception = new RuntimeSqlException(t);
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
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
    }

    interface ConnectionConsumer {
        void doInConnection(Connection connection) throws SQLException;
    }

}
