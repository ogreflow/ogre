package com.ws.common.logging;

import org.apache.log4j.Level;

import java.io.IOException;
import java.io.PrintStream;

/**
 * A subclass of PrintStream that redirects its output to a log4j Logger.
 *
 * <p>This class is used to map PrintStream/PrintWriter oriented logging onto
 *    the log4j Categories. Examples include capturing System.out/System.err
 *
 * @version <tt>$Revision: 1.3 $</tt>
 * @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>.
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 */
public class LoggerStream extends PrintStream {

    private org.apache.log4j.Logger logger;
    private Level level;
    private boolean issuedWarning;

    /**
     * Redirect logging to the indicated logger using Level.INFO
     */
    public LoggerStream(final org.apache.log4j.Logger logger)
    {
        this(logger, Level.INFO, System.out);
    }

    /**
     * Redirect logging to the indicated logger using the given
     * level. The ps is simply passed to super but is not used.
     */
    public LoggerStream(final org.apache.log4j.Logger logger,
                        final Level level,
                        final PrintStream ps)
    {
        super(ps);
        this.logger = logger;
        this.level = level;
    }

    public void println(String msg)
    {
        if( msg == null )
            msg = "null";
        byte[] bytes = msg.getBytes();
        write(bytes, 0, bytes.length);
    }

    public void println(Object msg)
    {
        if( msg == null )
            msg = "null";
        byte[] bytes = msg.toString().getBytes();
        write(bytes, 0, bytes.length);
    }

    public void write(byte b)
    {
        byte[] bytes = {b};
        write(bytes, 0, 1);
    }

    private ThreadLocal recursiveCheck = new ThreadLocal();
    public void write(byte[] b, int off, int len)
    {
        Boolean recursed = (Boolean)recursiveCheck.get();
        if (recursed != null && recursed.equals(Boolean.TRUE))
        {
                /* There is a configuration error that is causing looping. Most
                   likely there are two console appenders so just return to prevent
                   spinning.
                */
            if( issuedWarning == false )
            {
                String msg = "ERROR: invalid console appender config detected, console stream is looping";
                try
                {
                    out.write(msg.getBytes());
                }
                catch(IOException ignore)
                {
                }
                issuedWarning = true;
            }
            return;
        }

        // Remove the end of line chars
        while( len > 0 && (b[len-1] == '\n' || b[len-1] == '\r') && len > off )
            len --;

        // HACK, something is logging exceptions line by line (including
        // blanks), but I can't seem to find it, so for now just ignore
        // empty lines... they aren't very useful.
        if (len != 0)
        {
            String msg = new String(b, off, len);
            recursiveCheck.set(Boolean.TRUE);

            logger.log(level, msg);

            recursiveCheck.set(Boolean.FALSE);
        }
    }
}