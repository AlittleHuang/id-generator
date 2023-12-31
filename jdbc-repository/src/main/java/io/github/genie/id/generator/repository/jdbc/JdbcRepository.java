package io.github.genie.id.generator.repository.jdbc;

import io.github.genie.id.generator.repository.jdbc.JdbcDatabase.Locker;
import io.github.genie.id.generator.core.log.Log;
import io.github.genie.id.generator.core.support.IdGeneratorConfig;
import io.github.genie.id.generator.core.support.Repository;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class JdbcRepository implements Repository, AutoCloseable {

    private final Log log = Log.get(JdbcRepository.class);

    public static final int UNINITIALIZED_ID = -1;


    private static final String LOCK_KEY = randomKey();

    private final JdbcDatabase database;
    private final ConnectionProvider connectionProvider;
    private final ScheduledFuture<?> scheduled;
    private final long timeOffset;
    private final int lockExpirySeconds;
    private final ScheduledExecutorService scheduledExecutorService;
    private final int maxId;
    private boolean shutdownServiceOnClose;

    private volatile int id = UNINITIALIZED_ID;
    private Long difTime;


    public JdbcRepository(ConnectionProvider connectionProvider) {
        this(connectionProvider, (1 << IdGeneratorConfig.DEFAULT_DEVICE_ID_WIDTH) - 1);
    }

    public JdbcRepository(ConnectionProvider connectionProvider, int maxId) {
        this(
                Executors.newSingleThreadScheduledExecutor(),
                connectionProvider,
                new MysqlDatabase(),
                maxId,
                Duration.ofSeconds(20),
                Duration.ofSeconds(4)
        );
        shutdownServiceOnClose = true;
    }

    private JdbcRepository(ScheduledExecutorService scheduledExecutorService,
                           ConnectionProvider connectionProvider,
                           JdbcDatabase database,
                           int maxId,
                           Duration lockExpiryDuration,
                           Duration lockRenewalPeriod) {
        this.database = database;
        this.scheduledExecutorService = scheduledExecutorService;
        this.connectionProvider = connectionProvider;
        this.maxId = maxId;
        this.lockExpirySeconds = (int) lockExpiryDuration.getSeconds();
        proofreadingTime();
        executeDDL();
        acquireId();
        this.timeOffset = queryTimeOffset();
        this.scheduled = initScheduled(scheduledExecutorService, lockRenewalPeriod);
    }

    private long queryTimeOffset() {
        AtomicLong res = new AtomicLong();
        doInConnection(connection -> {
            long time = database.queryTimeOffsetConfig(connection);
            res.set(time);
        });
        return res.get();
    }

    private void executeDDL() {
        doInConnection(database::executeTableDDL);
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
            throw new IllegalStateException("id uninitialized or status closed");
        }
        return id;
    }

    private void renewExpiration() {
        long start = System.currentTimeMillis();
        doInConnection(connection -> {
            int updateRoles = database.lock(connection, id, LOCK_KEY, LOCK_KEY, lockExpirySeconds);
            if (updateRoles == 1) {
                commit(connection);
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
            doAcquireId();
            if (id == UNINITIALIZED_ID) {
                await();
            }
        }
    }

    protected void doAcquireId() {
        updateLock();
        if (id == UNINITIALIZED_ID) {
            newLock();
        }
    }

    protected void newLock() {
        doInConnection(connection -> {
            int nextId = database.queryNextId(connection);
            if (isAvailableId(nextId)) {
                if (database.newLock(connection, nextId, lockExpirySeconds, LOCK_KEY)) {
                    updateId(nextId);
                }
            }
        });
    }

    private boolean isAvailableId(int nextId) {
        return nextId >= 0 && nextId <= maxId;
    }

    protected void await() {
        AtomicLong awaitTime = new AtomicLong();
        doInConnection(connection -> {
            long time = database.queryAwaitTime(connection, maxId);
            awaitTime.set(time);
        });
        if (awaitTime.get() > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(awaitTime.get()));
        }
    }

    private void updateLock() {
        doInTransaction(connection -> {
            Locker record = database.queryExpiredForUpdate(connection);
            if (record != null && isAvailableId(record.getId())) {
                int lock = database.lock(connection, record.getId(), record.getKey(), LOCK_KEY, lockExpirySeconds);
                if (lock == 1) {
                    updateId(record.getId());
                    commit(connection);
                }
            }
        });
    }

    private static void commit(Connection connection) throws SQLException {
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    @Override
    public long getTimeOffset() {
        return timeOffset + difTime;
    }

    private void proofreadingTime() {
        doInConnection(connection -> {
            long start = System.currentTimeMillis();
            long dbTime = database.queryTime(connection);
            long end = System.currentTimeMillis();
            long dif = (end + start) / 2 - dbTime;
            log.trace(() -> "time dif: " + dif + "ms");
            if (difTime == null || difTime < dif) {
                difTime = dif;
                log.debug(() -> "update time dif: " + dif + "ms");
            }

        });
    }

    private void updateId(int id) {
        this.id = id;
        log.info(() -> "update id:" + id);
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

    public void doInTransaction(Connection connection, ConnectionConsumer connectionConsumer) {
        try {
            database.doInTransaction(connection, connectionConsumer);
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
        updateId(UNINITIALIZED_ID);
        scheduled.cancel(true);
        if (shutdownServiceOnClose) {
            scheduledExecutorService.shutdown();
        }
    }

}