package io.github.ifnesi.jdbckafka.cache;

import java.nio.file.Path;
import java.sql.Statement;
import java.util.Optional;

/**
 * Run by CacheStoreTest in a JVM of its own: opens a cache for cluster A, commits a row and stops without
 * closing anything, as a crashed process would, leaving the committed data in the WAL.
 */
public final class CacheCrash {

    public static void main(String[] args) throws Exception {
        CacheStore cache = CacheStore.open(Path.of(args[0]), Optional.of("A"));
        try (Statement s = cache.writer().createStatement()) {
            s.execute("INSERT INTO \"$subjects\" VALUES ('crash-value', 1, '1', 0)");
        }
        Runtime.getRuntime().halt(0);
    }
}
