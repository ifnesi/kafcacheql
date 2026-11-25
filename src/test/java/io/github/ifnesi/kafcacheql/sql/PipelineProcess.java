package io.github.ifnesi.kafcacheql.sql;

import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/** A whole query in a second JVM, paused after fetching its first batch and before taking the write lock. */
public final class PipelineProcess {
    private PipelineProcess() {}

    static DriverConfig config(Path file) throws SQLException {
        Properties p = new Properties();
        p.setProperty("cache.path", file.toString());
        p.setProperty("client.id", "pipeline-" + Integer.toHexString(file.toString().hashCode()));
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        p.setProperty("sync.batch.records", "1");
        p.setProperty("max.poll.records", "1");
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }

    static String sql(String topic) {
        return "SELECT _partition,count(*),sum(n) FROM " + StorageDdl.quote(topic)
                + " WHERE _offset>=0 GROUP BY _partition ORDER BY _partition";
    }

    public static void main(String[] args) throws Exception {
        try (var cache = CacheContext.acquire(config(Path.of(args[0])));
             var execution = new Execution(Duration.ofSeconds(30))) {
            cache.hooks(new SyncWorker.Hooks() {
                boolean first = true;
                @Override public void beforeCommit(SyncBatch batch) throws SQLException {
                    if (!first) return;
                    first = false;
                    System.out.println("BUFFERED");
                    try { if (System.in.read() != '\n') throw new SQLException("Missing query release"); }
                    catch (java.io.IOException e) { throw new SQLException(e); }
                }
            });
            System.out.println("RESULT " + QueryPipeline.execute(cache, sql(args[1]), Map.of(), execution).rows());
        }
    }
}
