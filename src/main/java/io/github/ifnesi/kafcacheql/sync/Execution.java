package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.config.DriverLoader;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** One execution's absolute monotonic deadline, cancellation, and whole-query replan budget. */
public final class Execution implements AutoCloseable {
    private static final ScheduledThreadPoolExecutor DEADLINES = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = new Thread(r, "kafcacheql-deadlines");
        t.setDaemon(true);
        t.setContextClassLoader(DriverLoader.LOADER);
        return t;
    });
    static { DEADLINES.setRemoveOnCancelPolicy(true); }

    @FunctionalInterface public interface Work<T> { T run() throws SQLException; }
    @FunctionalInterface public interface Subscription extends AutoCloseable { @Override void close(); }

    private final long deadline;
    private final boolean timed;
    private final Set<Runnable> listeners = new HashSet<>();
    private final ScheduledFuture<?> alarm;
    private boolean cancelled;
    private boolean expired;
    private boolean closed;
    private boolean replanned;

    /** Zero means no timeout, as in Statement.setQueryTimeout. */
    public Execution(Duration timeout) {
        if (timeout.isNegative()) throw new IllegalArgumentException("negative timeout");
        long nanos = timeout.toNanos();
        timed = nanos != 0;
        deadline = System.nanoTime() + nanos;
        alarm = timed ? DEADLINES.schedule(() -> stop(true), nanos, TimeUnit.NANOSECONDS) : null;
    }

    public void cancel() { stop(false); }

    private synchronized void stop(boolean timeout) {
        if (closed || cancelled || expired) return;
        if (timeout) expired = true; else cancelled = true;
        // Actions must be short and may not acquire execution ownership: worker callbacks only wake/remove.
        for (Runnable listener : listeners) listener.run();
    }

    public synchronized void check() throws SQLException {
        // A waiter can observe expiry before the scheduled alarm runs. Deliver the stop here too,
        // before its subscription is released, so an expired queued job is removed immediately.
        if (!cancelled && !closed && timed && System.nanoTime() - deadline >= 0) stop(true);
        if (expired) {
            throw new SQLTimeoutException("Query deadline expired", "HYT00");
        }
        if (cancelled || closed) throw new SQLException("Query cancelled", "57014");
        if (Thread.currentThread().isInterrupted()) {
            stop(false);
            throw new SQLException("Query interrupted", "57014");
        }
    }

    /** Also useful for Kafka APIs accepting a Duration; never extends this execution's deadline. */
    public synchronized Duration remaining(Duration maximum) throws SQLException {
        check();
        return timed ? Duration.ofNanos(Math.min(maximum.toNanos(), Math.max(1, deadline - System.nanoTime()))) : maximum;
    }

    public synchronized Subscription onStop(Runnable listener) throws SQLException {
        check();
        listeners.add(listener);
        return () -> { synchronized (Execution.this) { listeners.remove(listener); } };
    }

    /** Waits on this deadline/token without cancelling a shared admin future. */
    public <T> T await(Future<T> future) throws SQLException {
        try { return awaitFuture(future); }
        catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException sql) throw sql;
            if (e.getCause() instanceof Error error) throw error;
            throw new SQLException("Execution failed", e.getCause());
        }
    }

    public <T> T awaitFuture(Future<T> future) throws SQLException, ExecutionException {
        while (true) {
            check();
            try {
                T result = future.get(remaining(Duration.ofMillis(50)).toNanos(), TimeUnit.NANOSECONDS);
                check();
                return result;
            } catch (TimeoutException keepWaiting) {
                // A bounded wait lets cancellation cut short admin calls as well as queueing.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancel();
                throw new SQLException("Query interrupted", "57014", e);
            } catch (ExecutionException e) {
                check();
                throw e;
            }
        }
    }

    /** Reruns the whole caller-supplied query, not just its last topic. Offline never replans. */
    public <T> T run(boolean offline, Work<T> query) throws SQLException {
        while (true) {
            check();
            try { return query.run(); }
            catch (StaleGeneration stale) {
                if (offline || replanned) throw new SQLException("Cached window changed for topic " + stale.topic(), "40001", stale);
                replanned = true;
            }
        }
    }

    @Override public synchronized void close() {
        if (closed) return;
        stop(false);
        closed = true;
        listeners.clear();
        if (alarm != null) alarm.cancel(false);
    }
}
