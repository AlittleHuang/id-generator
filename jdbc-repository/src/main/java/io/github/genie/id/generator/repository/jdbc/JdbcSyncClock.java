package io.github.genie.id.generator.repository.jdbc;

import io.github.genie.id.generator.core.auto.SyncClock;
import io.github.genie.id.generator.core.log.Log;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public abstract class JdbcSyncClock implements SyncClock {
    public static final int DEFAULT_EXPIRY_SECONDS = 20;
    public static final Duration DEFAULT_LOCK_RENEWAL_PERIOD = Duration.ofSeconds(3);
    protected final Log log = Log.get(JdbcSyncClock.class);

    protected volatile SyncClock.Clock clock;
    protected static final String RANDOM_KEY = randomKey();

    protected final int maxId;
    protected final String key;
    protected final ConnectionProvider connectionProvider;
    protected final int expirySeconds;

    protected final long dbTimeOffset;

    protected final long startStamp;

    protected final Lock lock = new ReentrantLock();

    public JdbcSyncClock(int maxId, ConnectionProvider connectionProvider) {
        this(maxId, RANDOM_KEY, connectionProvider, DEFAULT_EXPIRY_SECONDS, DEFAULT_LOCK_RENEWAL_PERIOD, newService());
    }

    public JdbcSyncClock(int maxId,
                         String key,
                         ConnectionProvider connectionProvider,
                         int expirySeconds,
                         Duration lockRenewalPeriod,
                         ScheduledExecutorService scheduledExecutorService) {
        this.maxId = maxId;
        this.key = key;
        this.connectionProvider = connectionProvider;
        this.expirySeconds = expirySeconds;
        this.dbTimeOffset = getDbTimeOffset();
        this.startStamp = getStartTime();
        acquireId();
        initScheduled(scheduledExecutorService, lockRenewalPeriod);
    }

    private void initScheduled(ScheduledExecutorService service, Duration lockRenewalPeriod) {
        long period = lockRenewalPeriod.toMillis();
        service.scheduleAtFixedRate(this::keepLock, period, period, TimeUnit.MILLISECONDS);
    }


    public abstract long getAwaitTime(Connection connection, int maxId) throws SQLException;

    protected abstract boolean insertRecord(Connection connection, int id, String key, int expirySeconds) throws SQLException;

    protected abstract boolean renewTtl(Connection connection, int id, String oldKey, String newKey, int expirySeconds) throws SQLException;

    protected abstract Integer getNextId(Connection connection) throws SQLException;

    protected abstract Record getExpiredRecord(Connection connection, int maxId) throws SQLException;

    protected abstract long getStartTime(Connection connection);

    protected abstract long getDbTime(Connection connection) throws SQLException;


    public void acquireId() {
        lock.lock();
        try {
            while (isIdExpired()) {
                doInTransaction(connection -> {
                    acquireExistsId(connection);
                    if (isIdExpired()) {
                        acquireNewId(connection);
                    }
                });
                if (isIdExpired()) {
                    await();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    protected boolean isIdExpired() {
        return clock == null;
    }

    protected void acquireNewId(Connection connection) throws SQLException {
        Integer nextId = getNextId(connection);
        if (nextId != null) {
            if (insertRecord(connection, nextId, key, expirySeconds)) {
                updateClock(nextId);
            }
        }
    }

    private void updateClock(Integer nextId) {
        long expiry = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expirySeconds);
        clock = new Clock(nextId, dbServerTime(expiry));
    }

    protected void acquireExistsId(Connection connection) throws SQLException {
        Record record = getExpiredRecord(connection, maxId);
        if (record != null) {
            if (renewTtl(connection, record.getId(), record.getKey(), key, expirySeconds)) {
                updateClock(record.getId());
            }
        }
    }

    protected void await() {
        AtomicLong waitTime = new AtomicLong();
        doInConnection(connection -> waitTime.set(getAwaitTime(connection, maxId)));
        if (waitTime.get() > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(waitTime.get()));
        }
    }

    protected long getDbTimeOffset() {
        AtomicLong result = new AtomicLong();
        doInConnection(connection -> {
            Long dbTimeOffset = null;
            for (int i = 0; i < 8; i++) {
                long start = System.currentTimeMillis();
                long remote = getDbTime(connection);
                long end = System.currentTimeMillis();
                long local = (end + start) / 2;
                long dif = remote - local;
                if (dbTimeOffset == null || dbTimeOffset < dif) {
                    dbTimeOffset = dif;
                }
            }
            result.set(dbTimeOffset);
        });
        return result.get();
    }

    public long getStartTime() {
        AtomicLong time = new AtomicLong();
        doInConnection(connection -> {
            long startTime = getStartTime(connection);
            time.set(startTime);
        });
        return time.get();
    }

    protected void keepLock() {
        long start = System.currentTimeMillis();
        if (!isIdExpired()) {
            try {
                doInConnection(connection -> {
                    if (renewTtl(connection, clock.id(), key, key, expirySeconds)) {
                        updateClock(clock.id());
                    }
                });
            } catch (Exception e) {
                log.error("renew expiration failed", e);
            }
        }
        if (isIdExpired()) {
            try {
                acquireId();
            } catch (Exception e) {
                log.error("acquire id failed", e);
            }
        }
        log.trace(() -> "renew in " + (System.currentTimeMillis() - start) + "ms");
        log.trace(() -> "renew success: " + (!isIdExpired()));
    }


    protected static String randomKey() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = ByteBuffer.allocate(Long.BYTES * 2)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        return new BigInteger(1, bytes).toString(Character.MAX_RADIX);
    }

    protected void doInTransaction(ConnectionConsumer connectionConsumer) {
        doInConnection(connection -> doInTransaction(connection, connectionConsumer));
    }

    public void doInTransaction(Connection connection, ConnectionConsumer connectionConsumer) {
        try {
            doInTransaction0(connection, connectionConsumer);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    protected void doInConnection(@NotNull ConnectionConsumer connectionConsumer) {
        try (Connection connection = connectionProvider.getConnection()) {
            connectionConsumer.doInConnection(connection);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    protected void doInTransaction0(Connection connection, ConnectionConsumer consumer) throws SQLException {
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

    @NotNull
    private static ScheduledExecutorService newService() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        });
    }

    private long dbServerTime(long time) {
        return time + dbTimeOffset;
    }

    @Override
    public SyncClock.Clock acquire() {
        return clock;
    }

    class Clock implements SyncClock.Clock {

        private final int id;
        private final long expiry;

        public Clock(int id, long expiry) {
            this.id = id;
            this.expiry = expiry;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public long startStamp() {
            return startStamp;
        }

        @Override
        public long expiry() {
            return expiry;
        }

        @Override
        public long now() {
            return dbServerTime(System.currentTimeMillis());
        }
    }

}
