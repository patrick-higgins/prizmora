package net.prizmora;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class PrizmoraConfig {

    // This class is immutable, so everything is final

    // "DAD": some stupid Oracle Application Server thing that we don't really care about
    private final String dad;
    // Portion of URL which comes before the DAD
    private final String urlPrefix;

    // large response disk spooling parameters
    private final File spoolDirectory;
    private final int spoolThreshold;

    // HTTP parameters
    private final int listenPort;
    private final int threadPoolSize;
    private final boolean showErrors;
    private final File errorPage;

    // Database connection pool parameters
    private final String dbHost;
    private final String dbSid;
    private final int dbPort;
    private final String dbUsername;
    private final String dbPassword;
    private final boolean dbRollback;
    private final boolean dbTestOnRelease;
    private final int dbTestTimeout;
    private final int dbBusyTimeout;
    private final int dbIdleTimeout;
    private final int dbMinConnections;
    private final int dbMaxConnections;
    private final long dbTestInterval;
    private final long dbShutdownWaitTime;
    private final String dbCharset;
    private final boolean dbCacheProcedures;

    public PrizmoraConfig(String configFile) throws IOException, PrizmoraConfigException {
        Props props = new Props(configFile);

        // required properties
        this.dad = props.getString("dad");
        String urlPrefix = props.getString("urlPrefix");
        if (!urlPrefix.endsWith("/")) {
            urlPrefix += "/";
        }
        this.urlPrefix = urlPrefix;
        this.listenPort = props.getInt("listenPort");
        this.errorPage = new File(props.getString("errorPage"));
        this.dbHost = props.getString("dbHost");
        this.dbSid = props.getString("dbSid");
        this.dbUsername = props.getString("dbUsername");
        this.dbMinConnections = props.getInt("dbMinConnections");
        this.dbMaxConnections = props.getInt("dbMaxConnections");

        // optional properties
        this.threadPoolSize = props.getInt("threadPoolSize", 6);
        this.showErrors = props.getBool("showErrors", false);
        this.spoolDirectory = new File(props.getString("spoolDirectory", "/opt/apps/jakarta-tomcat/spool"));
        this.spoolThreshold = props.getInt("spoolThreshold", 1 * 1024 * 1024); // 1MB default
        this.dbPassword = props.getString("dbPassword", dbUsername);
        this.dbPort = props.getInt("dbPort", 1521);
        this.dbRollback = props.getBool("dbRollback", false);
        this.dbTestOnRelease = props.getBool("dbTestOnRelease", true);
        this.dbTestTimeout = props.getInt("dbTestTimeout", 15);
        this.dbBusyTimeout = props.getInt("dbBusyTimeout", 600);
        this.dbIdleTimeout = props.getInt("dbIdleTimeout", 600);
        this.dbTestInterval = props.getLong("dbTestInterval", 300);
        this.dbShutdownWaitTime = props.getLong("dbShutdownWaitTime", 60);
        this.dbCharset = props.getString("dbCharset", "iso-8859-1");
        this.dbCacheProcedures = props.getBool("dbCacheProcedures", true);

        if (!this.spoolDirectory.exists()) {
            throw new PrizmoraConfigException("spoolDirectory " + spoolDirectory + " does not exist");
        }

        if (!this.errorPage.exists()) {
            throw new PrizmoraConfigException("errorPage " + errorPage + " does not exist");
        }

        props.checkUnrecognized();
    }

    public String dad() { return dad; }
    public String urlPrefix() { return urlPrefix; }

    public int listenPort() { return listenPort; }
    public int threadPoolSize() { return threadPoolSize; }
    public boolean showErrors() { return showErrors; }
    public File errorPage() { return errorPage; }
    public File spoolDirectory() { return spoolDirectory; }
    public int spoolThreshold() { return spoolThreshold; }

    public String dbHost() { return dbHost; }
    public String dbSid() { return dbSid; }
    public int dbPort() { return dbPort; }
    public String dbUsername() { return dbUsername; }
    public String dbPassword() { return dbPassword; }
    public boolean dbRollback() { return dbRollback; }
    public boolean dbTestOnRelease() { return dbTestOnRelease; }
    public int dbTestTimeout() { return dbTestTimeout; }
    public int dbBusyTimeout() { return dbBusyTimeout; }
    public int dbIdleTimeout() { return dbIdleTimeout; }
    public int dbMinConnections() { return dbMinConnections; }
    public int dbMaxConnections() { return dbMaxConnections; }
    public long dbTestInterval() { return dbTestInterval; }
    public long dbShutdownWaitTime() { return dbShutdownWaitTime; }
    public String dbCharset() { return dbCharset; }
    public boolean dbCacheProcedures() { return dbCacheProcedures; }


    /**
     * Utility class to get typed and default values from java.util.Properties
     */
    private static class Props {

        private final Properties props = new Properties();
        private final Set<String> requestedNames = new HashSet<String>();

        public Props(String file) throws IOException {
            FileReader reader = null;
            try {
                reader = new FileReader(file);
                props.load(reader);
            }
            finally {
                IoUtil.close(reader);
            }
        }

        private void addName(String name) {
            requestedNames.add(name);
        }

        // we track each support property name by storing them as we read
        // their values. Then we check all the properties we are given to
        // see if any of them are not in the supported set.
        public void checkUnrecognized() throws PrizmoraConfigException {
            ArrayList<String> unrecognized = new ArrayList<String>();
            for (String name: props.stringPropertyNames()) {
                if (!requestedNames.contains(name)) {
                    unrecognized.add(name);
                }
            }
            if (unrecognized.size() > 0) {
                throw new PrizmoraConfigException("Unrecognized config parameter(s): " + unrecognized);
            }
        }

        public String getString(String name, String def) {
            addName(name);
            String val = props.getProperty(name);
            if (val == null) {
                return def;
            }
            return val;
        }

        public String getString(String name) {
            String val = getString(name, null);
            if (val == null) {
                throw new RuntimeException("Missing required config parameter: " + name);
            }
            return val;
        }

        public boolean getBool(String name, boolean def) {
            String val = getString(name, null);
            if (val == null) {
                return def;
            }
            return Boolean.valueOf(val);
        }

        public boolean getBool(String name) {
            return Boolean.valueOf(getString(name));
        }

        public int getInt(String name, int def) {
            String val = getString(name, null);
            if (val == null) {
                return def;
            }
            return Integer.parseInt(val);
        }

        public int getInt(String name) {
            return Integer.parseInt(getString(name));
        }

        public long getLong(String name, long def) {
            String val = getString(name, null);
            if (val == null) {
                return def;
            }
            return Long.parseLong(val);
        }

        public long getLong(String name) {
            return Long.parseLong(getString(name));
        }

    }

}
