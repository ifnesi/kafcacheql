package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Holds a real Kafka observation until the parent releases this competing process. */
public final class ValidityProcess {
    private ValidityProcess() {}
    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", args[0]);
        DriverConfig config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
        try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(30))) {
            SyncPlan observed = cache.plan(args[1], execution);
            System.out.println("READY");
            if (System.in.read() != '\n') throw new IllegalStateException("missing release");
            AtomicBoolean preparing = new AtomicBoolean(true);
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void beforePrepare(SyncPlan plan) throws java.sql.SQLException {
                    if (!preparing.getAndSet(false)) return;
                    System.out.println("PREPARING");
                    try {
                        if (System.in.read() != '\n') throw new java.sql.SQLException("missing prepare release");
                    } catch (java.io.IOException e) { throw new java.sql.SQLException(e); }
                }
            });
            AtomicBoolean first = new AtomicBoolean(true);
            long id = execution.run(false, () -> cache.sync(first.getAndSet(false) ? observed : cache.plan(args[1], execution), execution));
            System.out.println("RESULT " + id);
        }
    }
}
