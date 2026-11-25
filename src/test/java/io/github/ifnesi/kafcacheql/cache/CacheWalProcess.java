package io.github.ifnesi.kafcacheql.cache;

import java.nio.file.Path;
import java.util.Optional;

/** Keeps a committed WAL alive until the parent has completed its concurrent read-only inspection. */
public final class CacheWalProcess {
    private CacheWalProcess() {}
    public static void main(String[] args) throws Exception {
        try (CacheStore cache = CacheStore.open(Path.of(args[0]), Optional.of("A"));
             var s = cache.writer().createStatement()) {
            s.execute("UPDATE \"$meta\" SET value='B' WHERE name='cluster_id'");
            System.out.println("WAL");
            if (System.in.read() != '\n') throw new IllegalStateException("missing release");
        }
    }
}
