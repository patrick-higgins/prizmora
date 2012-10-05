package net.prizmora;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is a refactor of JdbcDBPrismConnectionCacheImpl adding
 * support for background connection testing, asynchronous release,
 * and minimized time spent holding locks. No JDBC operations are
 * performed while holding a lock.
 *
 * @author Patrick Higgins
 */
public class PrizmoraConnectionPool {

    private static final Logger log = LogManager.getLogger(PrizmoraConnectionPool.class);
    private static final Driver driver = new oracle.jdbc.OracleDriver();

    private static final int INIT = 0;
    private static final int FREE = 1;
    private static final int BUSY = 2;
    private static final int TEST = 3;

    private final PrizmoraConfig config;

    /* The following object is used as a lock.

       Synchronize on connectionList whenever accessing connectionList (except
       for logging) or changing the state of a CachedConnection.
    */
    private final List<CachedConnection> connectionList = new ArrayList<CachedConnection>();

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ExecutorService executor = Executors.newCachedThreadPool();

    public PrizmoraConnectionPool(PrizmoraConfig config) {
        this.config = config;
        scheduler.scheduleAtFixedRate(new PoolCleaner(), 1, 1, TimeUnit.SECONDS);
    }

    public Connection get() throws SQLException {
        CachedConnection conn = null;

        synchronized(connectionList) {
            for (CachedConnection ctmp: connectionList) {
                if (ctmp.state == FREE) {
                    conn = ctmp;
                    conn.state = BUSY;
                    break;
                }
            }
        }

        if (conn == null) {
            // if null, create new connection
            synchronized(connectionList) {
                if (connectionList.size() >= config.dbMaxConnections()) {
                    throw new SQLException("No more connections available");
                }

                log.info("Connecting to Oracle, pool size: {}", connectionList.size());

                conn = new CachedConnection(config.dbBusyTimeout());
                conn.state = INIT;
                connectionList.add(conn);
            }

            try {
                conn.connect(config);
            }
            catch (Throwable e) {
                synchronized(connectionList) {
                    connectionList.remove(conn);
                    log.error("Failed to connect to Oracle, pool size: {}", connectionList.size());
                }

                if (e instanceof SQLException) {
                    throw (SQLException) e;
                }
                if (e instanceof Error) {
                    throw (Error) e;
                }
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }

                throw (SQLException) new SQLException(e.getMessage()).initCause(e);
            }

            synchronized(connectionList) {
                conn.state = BUSY;
                log.info("Connected to Oracle, pool size: {}", connectionList.size());
            }
        }

        conn.counter = config.dbBusyTimeout();

        log.debug("Checked out conn {}", conn);

        return conn.sqlconn;
    }

    public void release() throws SQLException {
        log.info("Closing connection pool.");
        scheduler.shutdown();
        executor.shutdown();

        synchronized (connectionList) {
            List<Thread> releasers = new ArrayList<Thread>();
            for (CachedConnection cachedConn: connectionList) {
                releasers.add(closeAsync(cachedConn));
            }
            connectionList.clear();

            // give the threads some time to shutdown cleanly
            long startTime = System.currentTimeMillis();
            for (Thread releaser: releasers) {
                long waitTime = config.dbShutdownWaitTime() * 1000 - (System.currentTimeMillis() - startTime);
                if (waitTime < 1) {
                    break;
                }

                try {
                    releaser.join(waitTime);
                }
                catch (InterruptedException ignore) {}
            }

            // count the living and interrupt them
            int aliveCount = 0;
            for (Thread releaser: releasers) {
                if (releaser.isAlive()) {
                    aliveCount++;
                    releaser.interrupt();
                }
            }

            long waited = System.currentTimeMillis() - startTime;
            if (aliveCount > 0) {
                log.error("Waited {}ms for database connections to close cleanly. There are still {} connection(s) active, but exiting anyway.", waited, aliveCount);
            }
            else {
                log.info("Successfully closed connection pool in {}ms", waited);
            }
        }
    }

    public void release(Connection dbConn) throws SQLException {
        log.debug("Releasing {}", dbConn);
        executor.execute(new ConnReleaser(dbConn));
    }

    public String toString() {
        return poolString();
    }

    // Private helpers

    private String poolString() {
        return "PrizmoraConnectionPool@" + Integer.toHexString(System.identityHashCode(this))
            + ": pool size=" + connectionList.size();
    }

    private Thread closeAsync(CachedConnection conn) {
        Thread thread = new Thread(new ConnCloser(conn, null));
        // don't let closer threads prevent shutting down the JVM
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private void closeConn(CachedConnection conn) {
        if (config.dbRollback())
            DbUtil.rollback(conn.sqlconn);
        DbUtil.close(conn.sqlconn);

        log.info("Closed {}, pool size: {}", conn.sqlconn, connectionList.size());
        log.debug("Closed conn: {}", conn);
        conn.sqlconn = null;
    }

    private void releaseConn(Connection sqlconn) {
        if (config.dbRollback())
            DbUtil.rollback(sqlconn);
        DbUtil.setAutoCommit(sqlconn, false);

        CachedConnection conn = null;
        synchronized(connectionList) {
            for (CachedConnection cachedConn: connectionList) {
                if (cachedConn.sqlconn == sqlconn) {
                    conn = cachedConn;
                }
            }
        }
        if (conn == null) {
            log.error("Could not find CachedConnection for {}", sqlconn);
            return;
        }

        conn.counter = config.dbIdleTimeout();

        if (config.dbTestOnRelease() && !testConn(conn)) {
            synchronized(connectionList) {
                connectionList.remove(conn);
            }
            closeConn(conn);
        }
        else {
             // set counter again in case the test took a while
            conn.counter = config.dbIdleTimeout();
            synchronized(connectionList) {
                conn.state = FREE;
            }
            log.debug("Released conn {}", conn);
        }
    }

    private boolean testConn(CachedConnection conn) {
        log.debug("Testing {}", conn);

        if (conn.sqlconn == null) {
            // not sure how this happens, but it does!
            return false;
        }

        boolean passed = true;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.sqlconn.prepareStatement("select 1 from dual");
            ps.setQueryTimeout(config.dbTestTimeout());
            rs = ps.executeQuery();
            if (!rs.next()) {
                passed = false;
            }
        }
        catch (Throwable t) {
            passed = false;
            log.warn("Connection failed 'select 1 from dual' test: {}, exception: {}", conn, t);
        }
        finally {
            DbUtil.close(rs);
            DbUtil.close(ps);
            conn.testedAt = System.currentTimeMillis();
        }
        return passed;
    }

    // Inner classes

    private static class CachedConnection {
        // timeout value in seconds, depends on the PoolCleaner being run
        // once per second
        int counter = -1;
        Connection sqlconn;
        int state = INIT;
        long testedAt = 0;

        CachedConnection(int counter) {
            this.counter = counter;
        }

        void connect(PrizmoraConfig config) throws SQLException {
            Properties props = new Properties();
            props.setProperty("user", config.dbUsername());
            props.setProperty("password", config.dbPassword());
            sqlconn = driver.connect(String.format("jdbc:oracle:thin:@%s:%d:%s",
                    config.dbHost(), config.dbPort(), config.dbSid()), props);
            DbUtil.setAutoCommit(sqlconn, false);
        }

        public String toString() {
            return "CachedConnection@" + Integer.toHexString(this.hashCode())
                + ": counter=" + counter
                + " sqlconn=" + sqlconn
                + " testedAt=" + testedAt
                + " state=" + state;
        }
    }

    private abstract class ConnOperator implements Runnable {
        CachedConnection conn;
        ConnOperator(CachedConnection conn) {
            this.conn = conn;
        }
        public abstract void run();
    }

    private class ConnCloser extends ConnOperator {
        String msg;
        ConnCloser(CachedConnection conn, String msg) {
            super(conn);
            this.msg = msg;
        }
        public void run() {
            // msg gets logged here so logging isn't done while holding a lock
            if (msg != null)
                log.info(msg);
            closeConn(conn);
        }
    }

    private class ConnReleaser implements Runnable {
        Connection conn;
        ConnReleaser(Connection conn) {
            this.conn = conn;
        }
        public void run() {
            releaseConn(conn);
        }
    }

    private class ConnTester extends ConnOperator {
        ConnTester(CachedConnection conn) {
            super(conn);
        }
        public void run() {
            if (testConn(conn)) {
                synchronized(connectionList) {
                    conn.state = FREE;
                }
            }
            else {
                synchronized(connectionList) {
                    connectionList.remove(conn);
                }
                closeConn(conn);
            }
        }
    }

    private class PoolCleaner implements Runnable {
        public void run() {
            if (log.isDebugEnabled())
              log.debug("PoolCleaner running on " + poolString());
            long time = System.currentTimeMillis();

            synchronized(connectionList) {
                int closeable = 0; // count of timed out connections

                // find the timed-out connections
                for (Iterator<CachedConnection> i = connectionList.iterator(); i.hasNext(); ) {
                    CachedConnection conn = i.next();
                    conn.counter--;

                    if (conn.counter <= 0) {
                        if (conn.state == BUSY) {
                            i.remove();
                            executor.execute(new ConnCloser(conn, "Removing busy timed-out connection: " + conn));
                        }
                        else if (conn.state == INIT) {
                            i.remove();
                            if (conn.sqlconn != null) {
                                executor.execute(new ConnCloser(conn, "Removing uninitialized timed-out connection: " + conn));
                            }
                        }
                        else {
                            // either free or being tested, close candidate
                            closeable++;
                        }
                    }
                }

                // clip closeable so we don't dip below minConnections
                int maxCloseable = connectionList.size() - config.dbMinConnections();
                if (closeable > maxCloseable) {
                    closeable = maxCloseable;
                }

                // remove idle timed-out connections
                for (Iterator<CachedConnection> i = connectionList.iterator(); i.hasNext() && closeable > 0; ) {
                    CachedConnection conn = i.next();
                    if (conn.state == FREE || conn.state == TEST) {
                        i.remove();
                        closeable--;
                        executor.execute(new ConnCloser(conn, "Removing idle timed-out connection: " + conn));
                    }
                }

                // test all the free connections that haven't been tested in a while
                for (CachedConnection conn: connectionList) {
                    if ((conn.state == FREE) &&
                        ((time - conn.testedAt) > (config.dbTestInterval() * 1000)))
                    {
                        conn.state = TEST;
                        executor.execute(new ConnTester(conn));
                    }
                }
            }
        }
    }

}
