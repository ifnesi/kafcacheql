package io.github.ifnesi.jdbckafka.testkit;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
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
 * clients log, without printing them. Outside a capture it prints WARN and ERROR to stderr and drops the rest.
 * There is one buffer for the JVM: tests run one at a time (JUnit's default), and captures don't overlap.
 */
public final class LogCapture implements SLF4JServiceProvider {

    private static final List<String> LINES = new ArrayList<>();
    private static volatile boolean capturing;

    private final ILoggerFactory factory = CapturingLogger::new;
    private final IMarkerFactory markers = new BasicMarkerFactory();
    private final MDCAdapter mdc = new NOPMDCAdapter();

    /** Clears what was recorded and records from now on. */
    public static synchronized void start() {
        LINES.clear();
        capturing = true;
    }

    /** Stops recording; returns every line recorded since {@link #start()}. */
    public static synchronized List<String> stop() {
        capturing = false;
        return List.copyOf(LINES);
    }

    private static synchronized void record(String line) {
        if (capturing) {
            LINES.add(line);
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
            if (!capturing && level.toInt() >= Level.WARN.toInt()) {
                System.err.println(line);
            }
        }
    }
}
