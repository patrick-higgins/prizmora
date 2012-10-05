package net.prizmora;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import oracle.sql.CLOB;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simpleframework.http.Address;
import org.simpleframework.http.ContentType;
import org.simpleframework.http.Form;
import org.simpleframework.http.Path;
import org.simpleframework.http.Query;
import org.simpleframework.http.Request;

public class ProcedureCache {

    private static final Logger log = LogManager.getLogger(ProcedureCache.class);

    private final ConcurrentMap<String, ProcedureTypes> procTypesCache = new ConcurrentHashMap<String, ProcedureTypes>();
    private final PrizmoraConfig config;

    public ProcedureCache(PrizmoraConfig config) {
        this.config = config;
    }

    public Reader call(Request req, Connection conn) throws Exception {
        resetPackages(conn);
        setCGIVars(req, conn);

        // XXX check for upload
        ProcedureCall call = new ProcedureCall(req);
        dbCall(call, conn);
        return getGeneratedStream(conn);
    }

    private void resetPackages(Connection conn) throws SQLException {
        CallableStatement cs = null;
        try {
            cs = conn.prepareCall("BEGIN dbms_session.reset_package; END;");
            cs.execute();
        } finally {
            DbUtil.close(cs);
        }
    }

    private void setCGIVars(Request req, Connection conn) throws SQLException {
        CallableStatement cs = null;
        // we have at most 50 CgiVars, and 7 non-CGI values. Make room for
        // 65 to ensure we don't have to reallocate the array w/wiggle room.
        List<String> bindParams = new ArrayList<String>(65);
        StringBuffer command = new StringBuffer("DECLARE var_val owa.vc_arr;\n");
        command.append("  var_name owa.vc_arr;\n");
        command.append("  dummy_num_vals integer; \nBEGIN ");
        // Get dummy val, force to execute init code of the package
        // if not execute this call the global vars of packages of owa_init
        // and owa_cookie have null vals
        command.append("dummy_num_vals := owa.initialize;\n");
        String hostaddr = req.getClientAddress().getAddress().getHostAddress();
        // System.out.println(hostaddr);
        // !!! make faster right into "command" variable
        StringTokenizer st = new StringTokenizer(hostaddr, ".");
        for (int i = 1; st.hasMoreElements(); i++) {
            command.append("owa.ip_address(").append(i).append("):=?;\n");
            bindParams.add(st.nextToken());
        }
        // Set the owa.cgi_var_val and owa.cgi_var_name used by owa package
        // for example owa.get_service_path use the CGI var SCRIPT_NAME
        command.append(" owa.user_id:=?;\n").append(" owa.password:=?;\n")
                .append(" owa.hostname:=?;\n");
        bindParams.add(config.dbUsername());
        bindParams.add(config.dbPassword());
        bindParams.add(hostaddr);

        command.append("   htp.init;\n");
        CgiVars env = new CgiVars(req, config);
        for (int i = 0; i < env.size; i++) {
            command.append(" var_name(").append(i + 1).append("):=?;\n")
                .append(" var_val(").append(i + 1).append("):=?;\n");
            bindParams.add(env.names[i]);
            bindParams.add(env.values[i]);
        }
        command.append(" owa.init_cgi_env(?,var_name,var_val);\n ");
        // get authorization mode
        command.append("END;");
        String sql = command.toString();
        if (log.isDebugEnabled()) {
            log.debug("Executing: " + sql);
            for (String bindParam : bindParams) {
                log.debug("   with param " + bindParam);
            }
        }
        try {
            cs = conn.prepareCall(command.toString());
            int paramIndex = 1;
            for (String bindParam : bindParams) {
                cs.setString(paramIndex++, bindParam);
            }
            cs.setInt(paramIndex++, env.size);
            cs.execute();
        } finally {
            DbUtil.close(cs);
        }
    }

    private void dbCall(ProcedureCall call, Connection conn) throws Exception {
        String procName = call.name();

        // Checks for package that violates exclusion_list parameter
        // Pakckages that start with any of these values are considered with
        // high risk
        String[] excludeList = { "sys.", "owa", "dbms_", "htp." };
        for (int i = 0; i < excludeList.length; i++) {
            if (procName.toLowerCase().startsWith(excludeList[i].toLowerCase()))
                throw new SQLException("Not Authorized");
        }
        // parse all FORM input parameters and arrays set as PL/SQL arrays
        // Calling with constants - no prepared calls
        // Handling Case Insensitive args in PL/SQL and owa_image.point
        // Eg:
        // http://server:port/servlet/plsql/example.print?a=b
        // http://server:port/servlet/plsql/example.print?A=b
        // make the same call to the procedure example.print('b')
        // PLSQL runtime choose the correct procedure to call
        // Work with overload procedure and in/out parameters to.
        // Eg:
        // http://server:port/servlet/plsql/example.print?A=b
        // http://server:port/servlet/plsql/example.print?A=b&c=d
        // Build procedure call
        StringBuffer command = new StringBuffer(procName + "("); // Main calling
                                                                 // command
        StringBuffer decvar = new StringBuffer("DECLARE \n"); // we will declare
                                                              // array variables
                                                              // here
        StringBuffer setvar = new StringBuffer("BEGIN \n"); // we will set array
                                                            // variables here
        int foundcount = 0;
        ProcedureTypes procTypes = null;
        if (config.dbCacheProcedures()) {
            procTypes = procTypesCache.get(procName);
        }

        if (procTypes == null) {
            procTypes = new ProcedureTypes(procName, conn);
            if (config.dbCacheProcedures()) {
                procTypesCache.putIfAbsent(procName, procTypes);
            }
        }

        List<CsCallback> callbacks = new ArrayList<CsCallback>();
        int callbackIndex = 1;
        // Build procedure call parameter by parameter

        try {
            String[] real_args_list = call.parameterNames();
            for (int i = 0; i < real_args_list.length; i++) {
                String name_args = real_args_list[i];
                List<String> multi_vals = call.parameterValues(name_args);
                String argumentName = name_args.toLowerCase();
                if (argumentName.indexOf(".") > 0) {
                    argumentName = argumentName.substring(0, argumentName
                            .indexOf("."));
                }
                String argumentType = procTypes.getArgumentType(argumentName);
                if (argumentType == null) {
                    log.warn("Warning: argument {} not in procedure description {}", name_args, procName);
                    throw new SQLException(
                            procName
                                    + ": MANY PROCEDURES MATCH NAME, BUT NONE MATCHES SIGNATURE (parameter name '"
                                    + name_args + "')");
                }
                // System.out.println("Arg. name:" + name_args + " found type: "
                // + argumentType);
                if (argumentType.indexOf(".") > 0) { // ARRAY variable syntax:
                                                     // owner.type.subtype
                    if (name_args.indexOf(".") > 0) { // must be owa_image.point
                        if (name_args.toLowerCase().endsWith(".x")) { // Use
                                                                      // only
                                                                      // name.x
                                                                      // definition
                                                                      // and
                                                                      // ignore
                                                                      // name.y
                            // handle owa_image.point data type
                            name_args = name_args.substring(0, name_args
                                    .indexOf("."));
                            decvar.append("x_dbprism_internal_param_").append(
                                    foundcount).append(" owa_image.point;\n");
                            String val_x = call.getParameter(name_args + ".x");
                            String val_y = call.getParameter(name_args + ".y");
                            // the owa_image.point data type is a array of
                            // varchar index by binary integer
                            // Position 1 is args.x value
                            // Position 2 is args.y value
                            setvar.append("x_dbprism_internal_param_").append(
                                    foundcount).append("(1):=?; ");
                            callbacks.add(new StringSetter(callbackIndex++,
                                    val_x));
                            setvar.append("x_dbprism_internal_param_").append(
                                    foundcount).append("(2):=?; ");
                            callbacks.add(new StringSetter(callbackIndex++,
                                    val_y));
                            command.append(name_args).append(
                                    "=>x_dbprism_internal_param_").append(
                                    foundcount).append(",");
                        } else { // Skip .y definition
                            continue;
                        }
                    } else {
                        // System.out.println(name_args + " argumentType =" +
                        // argumentType);
                        for (String multi_val : multi_vals) {
                            setvar.append("x_dbprism_internal_param_").append(
                                    foundcount).append("(").append((i + 1))
                                    .append("):=?; ");
                            callbacks.add(new StringSetter(callbackIndex++,
                                    multi_val));
                        } // end for make array variable
                        command.append(name_args).append(
                                "=>x_dbprism_internal_param_").append(
                                foundcount).append(",");
                        // Oracle 10g replace SYS by PUBLIC when object where
                        // installed on sys schema and granted to public.
                        // Remove PUBLIC and use short version (package.type)
                        // for the argument type.
                        argumentType = argumentType.replaceFirst("^PUBLIC\\.",
                                "");
                        decvar.append("x_dbprism_internal_param_").append(
                                foundcount).append(" ").append(argumentType)
                                .append(";\n");
                    }
                } else { // otherwise, must be scalar type or cast to scalar
                    String s;
                    if (name_args.indexOf(".") > 0) {
                        if (name_args.toLowerCase().endsWith(".x")) { // Use
                                                                      // only
                                                                      // name.x
                                                                      // definition
                                                                      // and
                                                                      // ignore
                                                                      // name.y
                            s = call.getParameter(name_args);
                            name_args = name_args.substring(0, name_args
                                    .indexOf("."));
                            // System.out.println(
                            // "Casting from owa_image.point to varchar2");
                        } else { // Skip .y definition
                            continue;
                        }
                    } else if (multi_vals != null) {
                        s = new String(multi_vals.get(0).getBytes(
                                config.dbCharset()));
                    } else {
                        s = new String(call.getParameter(name_args).getBytes(
                                config.dbCharset()));
                    }
                    if ("CLOB".equalsIgnoreCase(argumentType)) {
                        setvar.append("x_dbprism_internal_param_").append(
                                foundcount).append(":=?; ");
                        callbacks.add(new ClobSetter(callbackIndex++, s, conn));
                        command.append(name_args).append(
                                "=>x_dbprism_internal_param_").append(
                                foundcount).append(",");
                        decvar.append("x_dbprism_internal_param_").append(
                                foundcount).append(" CLOB;\n");
                    } else {
                        setvar.append("x_dbprism_internal_param_").append(
                                foundcount).append(":=?; ");
                        callbacks.add(new StringSetter(callbackIndex++, s));
                        command.append(name_args).append(
                                "=>x_dbprism_internal_param_").append(
                                foundcount).append(",");
                        decvar.append("x_dbprism_internal_param_").append(
                                foundcount).append(" VARCHAR2(32767);\n");
                    }
                } // end if muti valued args
                foundcount++;
            }
            command = new StringBuffer(decvar.toString() + setvar.toString()
                    + command.toString().substring(0, command.length() - 1));
            if (foundcount == 0) {
                command.append("; END;");
            } else {
                command.append("); END;");
            }
            log.debug(".dbCall command: \n{}", command);
            // Exec procedure in DB
            CallableStatement cs = null;
            try {
                cs = conn.prepareCall(command.toString());
                for (CsCallback callback : callbacks) {
                    callback.callback(cs);
                }
                cs.execute();
            } catch (SQLException e) {
                throw new SQLException("PLSQL Adapter - PLSQL Error\n"
                        + e.getMessage() + msgArgumentCallError(call));
            } finally {
                DbUtil.close(cs);
            }
        } finally {
            for (CsCallback callback : callbacks) {
                callback.cleanup();
            }
        }
    }

    private String msgArgumentCallError(ProcedureCall call) throws Exception {
        StringBuffer text_error = new StringBuffer();
        text_error.append("\n\n\n While try to execute ").append(call.name());
        text_error.append("\n with args\n");
        String[] parameterNames = call.parameterNames();
        for (int i = 0; i < parameterNames.length; i++) {
            String name_args = parameterNames[i];
            List<String> multi_vals = call.parameterValues(name_args);
            if (multi_vals.size() > 1) { // must be owa_util.ident_array type
                text_error.append("\n").append(name_args).append(":");
                for (String val : multi_vals) {
                    text_error.append("\n\t").append(
                            new String(val.getBytes(config.dbCharset())));
                }
            } else if (name_args.indexOf('.') > 0) {
                // image point data type
                text_error.append("\n").append(
                        name_args.substring(0, name_args.indexOf('.'))).append(
                        ":");
                text_error.append("\n\t(").append(call.getParameter(name_args));
                name_args = parameterNames[++i];
                text_error.append(":")
                        .append(call.getParameter(name_args) + ")");
            } else {
                // scalar data type
                text_error.append("\n").append(name_args).append(":");
                text_error.append("\n\t").append(call.getParameter(name_args));
            }
        }
        return text_error.toString();
    }

    public Reader getGeneratedStream(Connection conn) throws SQLException {
        File spoolFile = null;
        FileWriter spoolWriter = null;
        DataFetcher fetcher = null;

        try {
            fetcher = new DataFetcher(conn);
            StringBuffer buff = new StringBuffer();

            // Get generated page in one call via stream
            String block;
            while ((block = fetcher.next()) != null) {
                if (spoolWriter != null) {
                    spoolWriter.write(block);
                } else {
                    buff.append(block);
                    if (buff.length() > config.spoolThreshold()) {
                        log.info("Spooling large response to disk");
                        spoolFile = File.createTempFile("page", null, config
                                .spoolDirectory());
                        spoolWriter = new FileWriter(spoolFile);
                        spoolWriter.write(buff.toString());
                    }
                }
            }

            if (spoolWriter != null) {
                spoolWriter.flush();
                return new FileReader(spoolFile);
            }

            return new StringReader(buff.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
            IoUtil.close(spoolWriter);
            if (spoolFile != null) {
                /*
                 * Delete the file so that it will go away when we close it. The
                 * contents of the file should still be readable by the open
                 * FileReader even though we've deleted it. This probably only
                 * works on a POSIX filesystem!
                 */
                spoolFile.delete();
            }
        }
    }

    private static class DataFetcher {
        private final static int MAX_PL_LINES = 127; // Max Lines

        private final CallableStatement cs;
        private boolean finished = false;

        public DataFetcher(Connection conn) throws SQLException {
            cs = conn.prepareCall("declare nlns number;\n"
                    + " buf_t varchar2(32767);\n" + " lines htp.htbuf_arr;\n"
                    + "begin\n" + "  nlns := ?;\n"
                    + "  OWA.GET_PAGE(lines, nlns);\n"
                    + "  if (nlns < 1) then\n" + "   buf_t := null;\n"
                    + "  else \n" + "   for i in 1..nlns loop\n"
                    + "     buf_t:=buf_t||lines(i);\n" + "   end loop;\n"
                    + "  end if;\n" + "  ? := buf_t; ? := nlns;\n" + "end;");
        }

        public String next() throws SQLException {
            if (finished) {
                return null;
            }

            cs.setInt(1, MAX_PL_LINES);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.registerOutParameter(3, Types.BIGINT);
            cs.execute();

            int nlines = cs.getInt(3);
            if (nlines < MAX_PL_LINES) {
                finished = true;
                if (nlines < 1) {
                    return null;
                }
            }
            return cs.getString(2);
        }

        public void close() {
            DbUtil.close(cs);
        }

    }

    private static interface CsCallback {
        void callback(CallableStatement cs) throws SQLException;

        void cleanup();
    }

    private static class StringSetter implements CsCallback {
        private int index;
        private String value;

        public StringSetter(int index, String value) {
            this.index = index;
            this.value = value;
        }

        public void callback(CallableStatement cs) throws SQLException {
            cs.setString(index, value);
        }

        public void cleanup() {
            // nothing to do
        }
    }

    private static class ClobSetter implements CsCallback {
        private int index;
        private String value;
        private CLOB clob;

        public ClobSetter(int index, String value, Connection sqlconn)
                throws SQLException {
            this.index = index;
            this.value = value;
            this.clob = CLOB.createTemporary(sqlconn, false,
                    CLOB.DURATION_SESSION);
        }

        public void callback(CallableStatement cs) throws SQLException {
            try {
                Writer iow = clob.setCharacterStream(0);
                iow.write(value);
                iow.flush();
                iow.close();
            } catch (IOException ioe) {
                throw new SQLException("Failed to write temporary CLOB:\n"
                        + ioe.getMessage());
            }
            cs.setClob(index, clob);
        }

        public void cleanup() {
            try {
                CLOB.freeTemporary(clob);
            } catch (Throwable t) {
                log.error("CLOB.freeTemporary error: {}", t.toString());
            }
        }
    }

    private static class CgiVars {

        public String[] names = new String[50];
        public String[] values = new String[50];
        public int size = 0;

        /**
         * Creates the class and stores all cgi environment variables in two
         * arrays.
         *
         * @param req
         *            HttpServletRequest - the request that initiated this call
         * @param connInfo
         *            ConnInfo - connection info
         * @param name
         *            String - username
         * @param pass
         *            String - password
         */
        public CgiVars(Request req, PrizmoraConfig config) {
            int n_size = 0;
            String argValue;

            Address address = req.getAddress();

            if ((argValue = req.getMethod()) != null) {
                names[n_size] = "REQUEST_METHOD";
                values[n_size++] = argValue;
            }

            /*
             * Don't support PATH_INFO or PATH_TRANSLATED if ((argValue =
             * req.getPathInfo()) != null) { names[n_size] = "PATH_INFO";
             * values[n_size++] = argValue; } if ((argValue =
             * req.getPathTranslated()) != null) { names[n_size] =
             * "PATH_TRANSLATED"; values[n_size++] = argValue; }
             */

            /*
             * Don't support Authorization: header names[n_size] =
             * "REMOTE_USER"; values[n_size++] = config.dbUsername(); if
             * ((argValue = req.getAuthType()) != null) { names[n_size] =
             * "AUTH_TYPE"; values[n_size++] = ((name.equals("") &&
             * pass.equals("")) ? argValue : "Basic"); }
             */

            Query query = address.getQuery();
            if (query != null && (argValue = query.toString()) != null) {
                names[n_size] = "QUERY_STRING";
                values[n_size++] = argValue;
            }

            Path path = address.getPath();

            if (path != null) {
                names[n_size] = "SCRIPT_NAME";
                values[n_size++] = path.getName();
            }

            names[n_size] = "SERVER_SOFTWARE";
            values[n_size++] = Prizmora.NAME;
            names[n_size] = "CONTENT_LENGTH";
            values[n_size++] = "" + req.getContentLength();

            ContentType contentType = req.getContentType();
            if (contentType != null
                    && (argValue = contentType.toString()) != null) {
                names[n_size] = "CONTENT_TYPE";
                values[n_size++] = argValue;
            }

            names[n_size] = "SERVER_PROTOCOL";
            values[n_size++] = "HTTP/" + req.getMajor() + "." + req.getMinor();

            String protocol = req.getValue("X-Forwarded-Proto");
            if (protocol == null) {
                protocol = address.getScheme();
            }

            if (protocol != null) {
                names[n_size] = "REQUEST_PROTOCOL";
                values[n_size++] = protocol.toUpperCase();
            }

            if ((argValue = req.getValue("host")) != null) {
                names[n_size] = "SERVER_NAME";
                values[n_size++] = argValue;
            }
            names[n_size] = "SERVER_PORT";
            values[n_size++] = "" + config.listenPort();

            String remoteAddr = req.getClientAddress().getAddress()
                    .getHostAddress();

            names[n_size] = "REMOTE_ADDR";
            values[n_size++] = remoteAddr;

            names[n_size] = "REMOTE_HOST";
            values[n_size++] = remoteAddr;

            if ((argValue = req.getValue("Referer")) != null) {
                names[n_size] = "HTTP_REFERER";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("User-Agent")) != null) {
                names[n_size] = "HTTP_USER_AGENT";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("Pragma")) != null) {
                names[n_size] = "HTTP_PRAGMA";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("Host")) != null) {
                names[n_size] = "HTTP_HOST";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("Accept")) != null) {
                names[n_size] = "HTTP_ACCEPT";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("Accept-Encoding")) != null) {
                names[n_size] = "HTTP_ACCEPT_ENCODING";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("Accept-Language")) != null) {
                names[n_size] = "HTTP_ACCEPT_LANGUAGE";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("Accept-Charset")) != null) {
                names[n_size] = "HTTP_ACCEPT_CHARSET";
                values[n_size++] = argValue;
            }
            if ((argValue = req.getValue("If-Modified-Since")) != null) {
                names[n_size] = "HTTP_IF_MODIFIED_SINCE";
                values[n_size++] = argValue;
                DateFormat df = new SimpleDateFormat(
                        "EEE, dd MMM yyyy HH:mm:ss 'GMT'", java.util.Locale.US);
                try {
                    java.util.Date lastClientMod = df.parse(argValue);
                    if (log.isDebugEnabled())
                        log.debug("If-Modified-Since:" + lastClientMod.getTime() / 1000 * 1000);
                } catch (Exception e) {
                    log.warn(".CgiVars - Error parsing If-Modified-Since header", e);
                }
            }
            if ((argValue = req.getValue("Cookie")) != null) {
                names[n_size] = "HTTP_COOKIE";
                values[n_size++] = argValue;
            }
            names[n_size] = "DAD_NAME";
            values[n_size++] = config.dad();
            names[n_size] = "DOC_ACCESS_PATH";
            values[n_size++] = "docs";
            names[n_size] = "REQUEST_CHARSET";
            values[n_size++] = config.dbCharset();
            names[n_size] = "DOCUMENT_TABLE";
            values[n_size++] = "owa_public.wpg_document";
            names[n_size] = "PLSQL_GATEWAY";
            values[n_size++] = Prizmora.NAME;
            names[n_size] = "GATEWAY_IVERSION";
            values[n_size++] = Prizmora.VERSION;
            names[n_size] = "REQUEST_IANA_CHARSET";
            values[n_size++] = config.dbCharset();
            size = n_size;
        }
    }

    private static class ProcedureTypes {

        // final for effective immutability and safe publication
        private final Map<Integer, Map<String, String>> overloads;

        /**
         * Find the Stored Procedure in the table all_arguments to get public
         * definitios If there are public Stored Procedures add this definition
         * to the Hashtable of the superclass, and store all overloaded
         * ocurrence of the same StoreProcedure
         */
        public ProcedureTypes(String procname, Connection sqlconn)
                throws SQLException {
            log.debug(".create overload for: '{}'", procname);

            // build map first, then publish it as a last step for thread
            // safety.
            Map<Integer, Map<String, String>> procedures = new HashMap<Integer, Map<String, String>>();

            String owner = null;
            String plpackage = null;
            String plprocedure = null;
            CallableStatement css = null;
            try {
                css = sqlconn
                        .prepareCall("BEGIN \n dbms_utility.name_resolve(?,1,?,?,?,?,?,?); \nEND;");
                css.setString(1, procname);
                css.registerOutParameter(2, Types.VARCHAR);
                css.registerOutParameter(3, Types.VARCHAR);
                css.registerOutParameter(4, Types.VARCHAR);
                css.registerOutParameter(5, Types.VARCHAR);
                css.registerOutParameter(6, Types.VARCHAR);
                css.registerOutParameter(7, Types.VARCHAR);
                css.execute();
                owner = css.getString(2);
                plpackage = css.getString(3);
                plprocedure = css.getString(4);
            } catch (SQLException e) {
                log.error("Caught an exception running dbms_utility.name_resolve() for the procedure named '{}' {}", procname, e);
                throw e;
            } finally {
                DbUtil.close(css);
            }

            PreparedStatement ps = null;
            ResultSet rs = null;

            try {
                if (plpackage == null) {
                    String sql = "SELECT argument_name, overload, data_type, type_owner, type_name, type_subname FROM all_arguments WHERE "
                        + " owner = ? AND package_name IS NULL AND object_name = ?";
                    ps = sqlconn.prepareStatement(sql);
                    log.debug("Executing: {}\nWith arg 1: {}\nWith arg 2: {}", sql, owner, plprocedure);
                    ps.setString(1, owner);
                    ps.setString(2, plprocedure);
                }
                else {
                    String sql = "SELECT argument_name, overload, data_type, type_owner, type_name, type_subname FROM all_arguments WHERE "
                        + " owner = ? AND package_name = ? AND object_name = ?";
                    ps = sqlconn.prepareStatement(sql);
                    log.debug("Executing: {}\nWith arg 1: {}\nWith arg 2: {}\nWith arg 3: {}", sql, owner, plpackage, plprocedure);
                    ps.setString(1, owner);
                    ps.setString(2, plpackage);
                    ps.setString(3, plprocedure);
                }
                rs = ps.executeQuery();

                Map<String, String> procedure = null;
                int old_overload = -1;
                while (rs.next()) {
                    String argument_name = rs.getString(1);
                    int overload = 1;
                    String overloadStr = rs.getString(2);

                    if (overloadStr != null) {
                        overload = Integer.parseInt(overloadStr);
                    }
                    if (old_overload != overload) {
                        procedure = new HashMap<String, String>();
                        procedures.put(overload, procedure);
                        old_overload = overload;
                    }
                    // if procedure has no argument, empty row is returned
                    if (argument_name == null) {
                        log.debug("            overload: {} no argument", overload);
                        continue;
                    }
                    argument_name = argument_name.toLowerCase();
                    String data_type = rs.getString(3);
                    String argumentType = data_type;
                    if ("PL/SQL TABLE".equals(argumentType)) { // argument is
                                                               // ARRAY variable
                        String type_owner = rs.getString(4);
                        String type_name = rs.getString(5);
                        String type_subname = rs.getString(6);
                        argumentType = type_owner + "." + type_name + "."
                                + type_subname;
                        rs.next(); // skip the next result...not sure why
                                   // though!
                    }
                    procedure.put(argument_name, argumentType);
                    log.debug("            overload: {} arg: {} data_type: {}",
                              overload, argument_name, argumentType);
                }
            } finally {
                DbUtil.close(rs);
                DbUtil.close(ps);
            }

            // safely publish mutable field by assigning to a final
            this.overloads = procedures;
        }

        public String getArgumentType(String argumentName) {
            int i = 1;
            String type;
            do {
                Map<String, String> procedure = overloads.get(i++);
                if (procedure == null) {
                    return null;
                }
                type = procedure.get(argumentName);
            } while (type == null);
            return type;
        }

    }

    private static class ProcedureCall {

        private final String name;
        private final String[] parameterNames;
        private final Map<String, List<String>> parameterValues;

        public ProcedureCall(Request req) throws Exception {
            Form form = req.getForm();
            Set<String> formKeys = form.keySet();
            String[] args = formKeys.toArray(new String[formKeys.size()]);
            Arrays.sort(args);
            Map<String, List<String>> values = new HashMap<String, List<String>>();

            String name = req.getPath().getName();
            if (name.charAt(0) == '!') { // flexible request
                final String NAME_ARRAY = "name_array";
                final String VALUE_ARRAY= "value_array";
                name = name.substring(1);

                List<String> nameArray = new ArrayList<String>();
                List<String> valueArray = new ArrayList<String>();
                for (int i = 0; i < args.length; i++) {
                    List<String> argValues = form.getAll(args[i]);
                    for (String argValue: argValues) {
                        nameArray.add(args[i]);
                        valueArray.add(argValue);
                    }
                }
                if (!nameArray.isEmpty()) {
                    values.put(NAME_ARRAY, nameArray);
                    values.put(VALUE_ARRAY, valueArray);
                }

                args = new String[] { NAME_ARRAY, VALUE_ARRAY };
            }
            else {
                for (int i = 0; i < args.length; i++) {
                    values.put(args[i], form.getAll(args[i]));
                }
            }

            // List<String> multi_vals = form.getAll(name_args);
            this.name = name;
            this.parameterNames = args;
            this.parameterValues = values;
        }

        public String name() {
            return name;
        }

        public String[] parameterNames() {
            return parameterNames;
        }

        public List<String> parameterValues(String parameterName) {
            return parameterValues.get(parameterName);
        }

        public String getParameter(String name) {
            List<String> list = parameterValues.get(name);
            if (list == null || list.size() < 1) {
                return null;
            }
            return list.get(0);
        }

    }

}
