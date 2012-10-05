package net.prizmora;

import java.sql.*;
import javax.sql.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Routines used to perform common JDBC operations without throwing exceptions,
 * but logging them.
 */
public final class DbUtil {
    private static final Logger log = LogManager.getLogger(DbUtil.class);

    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void close(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void close(XAConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.rollback();
                }
            }
            catch (Throwable t) {
                log.warn("rollback error: {}", t.toString());
            }
        }
    }

    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            }
            catch (Throwable t) {
                log.warn("commit error: {}", t.toString());
            }
        }
    }

    public static void setAutoCommit(Connection conn, boolean autoCommit) {
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.setAutoCommit(autoCommit);
                }
            }
            catch (Throwable t) {
                log.warn("setAutoCommit error: {}", t.toString());
            }
        }
    }

}
