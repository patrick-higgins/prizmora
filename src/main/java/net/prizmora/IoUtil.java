package net.prizmora;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class IoUtil {

    private static final Logger log = LogManager.getLogger(IoUtil.class);

    public static void close(InputStream s) {
        if (s != null) {
            try {
                s.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void close(OutputStream s) {
        if (s != null) {
            try {
                s.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void close(Reader s) {
        if (s != null) {
            try {
                s.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

    public static void close(Writer s) {
        if (s != null) {
            try {
                s.close();
            }
            catch (Throwable t) {
                log.warn("close error: {}", t.toString());
            }
        }
    }

}
