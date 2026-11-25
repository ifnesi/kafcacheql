package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Path;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;

/** Real competing process. stdin controls its write-lock lifetime, not elapsed sleeps. */
public final class SyncProcess {
    private SyncProcess() {}

    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", args[1]); p.setProperty("sync.batch.records", "2");
        DriverConfig config = DriverConfig.load("jdbc:kafcacheql://" + args[3], p);
        try (KafkaClients.Lease clients = KafkaClients.acquire(config)) {
            if (args[0].equals("lock")) {
                try (CacheStore cache = CacheStore.open(Path.of(args[1]), Optional.of(clients.clients().clusterId()));
                     Statement s = cache.writer().createStatement()) {
                    s.execute("BEGIN IMMEDIATE");
                    System.out.println("LOCKED");
                    if (System.in.read() != '\n') throw new IllegalStateException("missing release");
                    s.execute("COMMIT");
                }
            } else if (args[0].equals("replace")) {
                // §9's rebuild policy is 0007a. This fixture replaces a topic via the already-tested
                // deletion/registration protocol, while the parent's worker is paused between batches.
                TestStack.recreateTopic(args[2], 1, Map.of());
                TestStack.sendString(args[2], "new", "new-generation", null);
                try (CacheStore cache = CacheStore.open(Path.of(args[1]), Optional.of(clients.clients().clusterId()));
                     var reader = cache.newReader()) {
                    long old = TopicRegistry.generation(reader, args[2]).orElseThrow();
                    delete(cache, args[2], old);
                    TopicRegistry.register(cache, OptionalLong.of(old), clients.clients().describeTopic(args[2], config).orElseThrow(), TopicSettings.NONE).orElseThrow();
                }
                try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(20))) {
                    System.out.println("REPLACED " + cache.sync(cache.plan(args[2], execution), execution));
                }
            } else if (args[0].equals("register-delete")) {
                long id;
                try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(20))) {
                    id = cache.sync(cache.plan(args[2], execution), execution);
                }
                TestStack.deleteTopic(args[2]);
                try (CacheStore cache = CacheStore.open(Path.of(args[1]), Optional.of(clients.clients().clusterId()))) {
                    delete(cache, args[2], id);
                }
                System.out.println("DELETED " + id);
            } else throw new IllegalArgumentException(args[0]);
        }
    }

    /** The later validity worker's deletion fixture; its retained row guards a stale absent observation. */
    private static void delete(CacheStore cache, String topic, long id) throws java.sql.SQLException {
        CacheStore.writeTransaction(cache.writer(), () -> {
            try (Statement s = cache.writer().createStatement()) {
                s.execute("DROP VIEW \"" + topic + "\""); s.execute("DROP TABLE \"$t" + id + "\"");
                s.execute("DELETE FROM \"$ranges\" WHERE topic_ref=" + id);
                s.execute("DELETE FROM \"$partitions\" WHERE topic_ref=" + id);
                s.execute("UPDATE \"$topics\" SET deleted_ms=1 WHERE id=" + id);
            }
            return null;
        });
    }
}
