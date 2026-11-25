package io.github.ifnesi.kafcacheql.sql;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Commits one partition, then lets the parent take its read snapshot before finishing the other. */
public final class WindowProcess {
    private WindowProcess() {}
    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", args[0]); p.setProperty("sync.batch.records", "1");
        var config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
        try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(30))) {
            System.out.println("READY");
            release();
            AtomicBoolean first = new AtomicBoolean(true);
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch batch) throws SQLException {
                    if (batch.records().isEmpty() || !first.getAndSet(false)) return;
                    System.out.println("COMMITTED " + batch.partition()); release();
                }
            });
            long id = execution.run(false, () -> cache.sync(cache.plan(args[1], execution), execution));
            System.out.println("DONE " + id);
        }
    }
    private static void release() throws SQLException {
        try { if (System.in.read() != '\n') throw new SQLException("Missing process release"); }
        catch (java.io.IOException e) { throw new SQLException(e); }
    }
}
