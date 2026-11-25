package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

/** An independently planned Kafka fetch held outside the write transaction by the parent process. */
public final class BatchProcess {
    private BatchProcess() {}

    public static void main(String[] args) throws Exception {
        if (args[0].equals("lock")) {
            try (var store = CacheStore.open(Path.of(args[1]), java.util.Optional.empty());
                 var s = store.writer().createStatement()) {
                s.execute("BEGIN IMMEDIATE");
                System.out.println("LOCKED");
                if (System.in.read() != '\n') throw new SQLException("missing release");
                s.execute("COMMIT");
            }
            return;
        }
        Properties p = new Properties();
        p.setProperty("cache.path", args[1]); p.setProperty("sync.batch.records", "2");
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        var config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
        try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(60))) {
            cache.hooks(new SyncWorker.Hooks() {
                boolean first = true;
                @Override public void beforeCommit(SyncBatch batch) throws SQLException {
                    if (!first) return;
                    first = false;
                    System.out.println("BUFFERED");
                    try { if (System.in.read() != '\n') throw new SQLException("missing release"); }
                    catch (java.io.IOException e) { throw new SQLException(e); }
                }
            });
            cache.sync(cache.plan(args[2], execution), execution);
        }
    }
}
