package io.github.ifnesi.kafcacheql.testkit;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Deque;
import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.LegacyAbstractLogger;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

/**
 * The tests' SLF4J provider (registered in src/test/resources/META-INF/services): records every log line,
 * at every level, between {@link #start()} and {@link #stop()}, so tests can check what the driver and the
 * clients log. WARN and ERROR always print to stderr, captured or not: nothing is hidden. Captures may nest;
 * each records its whole lifetime, including logs recorded by an inner capture.
 */
public final class LogCapture implements SLF4JServiceProvider {

    private static final Deque<Capture> CAPTURES = new ArrayDeque<>();
    private static volatile boolean capturing;

    private final ILoggerFactory factory = CapturingLogger::new;
    private final IMarkerFactory markers = new BasicMarkerFactory();
    private final MDCAdapter mdc = new NOPMDCAdapter();

    public static final class Capture implements AutoCloseable {
        private final List<String> lines = new ArrayList<>();
        public List<String> lines() { synchronized (LogCapture.class) { return List.copyOf(lines); } }
        @Override public void close() {
            synchronized (LogCapture.class) {
                CAPTURES.remove(this);
                capturing = !CAPTURES.isEmpty();
            }
        }
    }

    public static synchronized Capture capture() {
        var capture = new Capture();
        CAPTURES.push(capture);
        capturing = true;
        return capture;
    }

    /** Records all levels until {@link #stop()}. */
    public static void start() { capture(); }

    /** Ends the innermost capture. */
    public static synchronized List<String> stop() {
        if (CAPTURES.isEmpty()) return List.of();
        var capture = CAPTURES.peek();
        capture.close();
        return capture.lines();
    }

    /** Also accepts WARN/ERROR lines forwarded from a forked test process. */
    public static synchronized void record(String line) {
        for (var capture : CAPTURES) {
            capture.lines.add(line);
        }
        if (line.startsWith("WARN ") || line.startsWith("ERROR ")) {
            System.err.println(line);
        }
    }

    @Override
    public ILoggerFactory getLoggerFactory() {
        return factory;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markers;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdc;
    }

    @Override
    public String getRequestedApiVersion() {
        return "2.0.99";
    }

    @Override
    public void initialize() {}

    private static final class CapturingLogger extends LegacyAbstractLogger {

        private static final long serialVersionUID = 1L;

        CapturingLogger(String name) {
            this.name = name;
        }

        private static boolean enabled(Level level) {
            return capturing || level.toInt() >= Level.WARN.toInt();
        }

        @Override public boolean isTraceEnabled() { return enabled(Level.TRACE); }
        @Override public boolean isDebugEnabled() { return enabled(Level.DEBUG); }
        @Override public boolean isInfoEnabled() { return enabled(Level.INFO); }
        @Override public boolean isWarnEnabled() { return enabled(Level.WARN); }
        @Override public boolean isErrorEnabled() { return enabled(Level.ERROR); }

        @Override
        protected String getFullyQualifiedCallerName() {
            return null;
        }

        @Override
        protected void handleNormalizedLoggingCall(Level level, Marker marker, String pattern, Object[] args,
                                                   Throwable throwable) {
            String line = level + " " + name + ": " + MessageFormatter.basicArrayFormat(pattern, args);
            if (throwable != null) {
                StringWriter trace = new StringWriter();
                throwable.printStackTrace(new PrintWriter(trace));
                line += "\n" + trace;
            }
            record(line);
        }
    }
}
