package net.prizmora;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;

public class Prizmora implements Container {

    public static final String NAME = "Prizmora";
    public static final String VERSION = "0.1";

    private static final Logger log = LogManager.getLogger(Prizmora.class);

    private final PrizmoraConfig config;
    private final PrizmoraConnectionPool pool;
    private final ProcedureCache procCache;
    private final String dadPath;

    public Prizmora(PrizmoraConfig config) throws Exception {
        this.config = config;
        this.pool = new PrizmoraConnectionPool(config);
        this.procCache = new ProcedureCache(config);
        this.dadPath = "/ce/" + config.dad() + "/";
    }

    @Override
    public void handle(Request req, Response resp) {
        try {
            if (req.getPath().getDirectory().equals(dadPath)) {
                log.debug("Handling request: {}", req);
                doHandle(req, resp);
            }
            else {
                log.debug("Ignoring request: {}", req);
                sendNotFound(req, resp);
            }
        }
        catch (Exception e) {
            log.error("handle error: {}", e.toString());
            if (config.showErrors()) {
                showError(resp, e);
            }
            else {
                sendErrorPage(req, resp);
            }
        }
        finally {
            try {
                resp.close();
            } catch (Exception e) {
                log.error("error closing response: {}", e.toString());
            }
        }
    }

    private void sendNotFound(Request req, Response resp) {
        try {
            resp.setCode(404);
            resp.setText("Not Found");
            PrintStream out = resp.getPrintStream();
            out.print("Not Found: " + req.getPath().getDirectory());
        }
        catch (Exception e) {
            log.error("error sending not found response: {}", e.toString());
        }
    }

    private void sendErrorPage(Request req, Response resp) {
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(config.errorPage());
            FileChannel fileChannel = inputStream.getChannel();

            resp.setCode(500);
            resp.setText("Internal Server Error");
            resp.setContentLength((int) fileChannel.size());
            resp.set("Content-Type", "text/html");

            WritableByteChannel respChannel = resp.getByteChannel();
            fileChannel.transferTo(0, fileChannel.size(), respChannel);
        }
        catch (Exception e) {
            log.error("error sending error page: {}", e.toString());
        }
        finally {
            IoUtil.close(inputStream);
        }
    }

    private void showError(Response resp, Exception e) {
        try {
            resp.setCode(500);
            PrintStream out = resp.getPrintStream();
            out.print(e.getMessage());
            e.printStackTrace(out);
            out.flush();
        }
        catch (Exception ex) {
            log.error("error showing error: {}", ex.toString());
        }
    }

    private void doHandle(Request req, Response resp) throws Exception {
        Reader pageReader = null;

        try {
            java.sql.Connection conn = pool.get();
            try {
                pageReader = procCache.call(req, conn);
            }
            finally {
                pool.release(conn);
            }
            showPage(pageReader, req, resp);
        }
        finally {
            IoUtil.close(pageReader);
        }
    }

    private void showPage(Reader pageReader, Request req, Response res) throws Exception {
        res.setCode(200);
        Charset charset = Charset.forName("UTF-8");
        WritableByteChannel out = res.getByteChannel();
        char[] buff_out = new char[8192];

        BufferedReader in = new BufferedReader(pageReader, 8192);
        boolean contentType = false;
        int i;
        String s = in.readLine();
        if (s != null && (s.startsWith("Location: ") ||
                          s.startsWith("Set-Cookie: ") ||
                          s.startsWith("Content-type: ") ||
                          s.startsWith("Status: "))) {
            // Verify if the position 1..n have the Syntax "xxx : yyy"
            // handle special case of Cookie definition or Content-type, or redirect
            // generated by owa_cookie.send or owa_util.mime_header
            // other header definitions are pased as is
            do { // Process each line of header
                //System.out.println("header: "+s);
                if (s.startsWith("Location: ")) { // Sent redirect
                    s = s.substring(10 /* "Location: ".length */);
                    /* XXX
                       if (!s.startsWith("/")) // Convert relative path to absolute, fix warkaround with HTMLDB
                       s = req.getContextPath()+"/"+ConnInfo.getURI(req)+"/"+s;
                    */
                    // LXG: changed to static access
                    res.set("Location", s);
                    res.setCode(302);
                    log.trace(".showPage redirect to Location: {}", s);
                    return;
                } else if (s.startsWith("Set-Cookie: ")) { // Makes cookies
                    // Parse the cookie line
                    log.trace(".showPage output cookie: {}", s);
                    /* XXX
                     * Cookie choc_chip = Make_Cookie(s.substring(12));
                     * res.addCookie(choc_chip);
                     */
                    res.set("Set-Cookie", s.substring(12 /* "Set-Cookie: ".length */));
                } else if (s.startsWith("Content-type: ")) { // Set content type
                    if (log.isTraceEnabled())
                        log.trace(".showPage setting Content-type: {}", s.substring(14 /* "Content-type: ".length */).trim());
                    res.set("Content-Type", s.substring(14 /* "Content-Type: ".length */).trim() + "; charset=UTF-8");
                    contentType = true;
                } else {
                    // if not Cookie definition translate as is
                    try {
                        // if it isn't a cookie it's another header info
                        log.trace(".showPage setting other header: {}", s);
                        res.set(s.substring(0, s.indexOf(':')), s.substring(s.indexOf(':') + 2));
                    } catch (Exception e) {
                        log.error(".showPage failed to parse the header '{}': {}", s, e);
                    }
                } // End if cookie
            } while ((s = in.readLine()) != null && s.length() > 0); // End while header lines
            if (!contentType)
                res.set("Content-Type", "text/html; charset=UTF-8");
        } else {
            // if not header syntax, print it as is
            // Set default Content-type
            res.set("Content-Type", "text/html; charset=UTF-8");
            out.write(charset.encode(s));
        }

        // Output the rest of generated page in htp.htbuf
        // send it without paying attention to new lines
        while ((i = in.read(buff_out)) > 0) {
            out.write(charset.encode(CharBuffer.wrap(buff_out, 0, i)));
        }

        out.close();
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage: "+Prizmora.class.getName()+" configFileName");
            System.exit(1);
        }

        PrizmoraConfig config = new PrizmoraConfig(args[0]);

        Container container = new Prizmora(config);
        ContainerServer server = new ContainerServer(container, config.threadPoolSize());
        Connection connection = new SocketConnection(server);
        SocketAddress address = new InetSocketAddress(config.listenPort());

        connection.connect(address);
        log.warn("Prizmora listening on {}", config.listenPort());
    }

}
