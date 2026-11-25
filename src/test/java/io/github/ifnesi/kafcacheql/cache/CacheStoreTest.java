package io.github.ifnesi.kafcacheql.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConfig;

class CacheStoreTest {

    private static final Optional<String> A = Optional.of("cluster-A_1");
    private static final Optional<String> B = Optional.of("cluster-B");

    @TempDir
    Path dir;

    /** PLAN §6.1: "Before anything writes, the file is checked on a read-only connection";
     * §16: "another process creating the WAL after the open's existence check and before its read-only look".
     */
    @Test
    void aWalCreatedByAnotherProcessDuringInspectionIsReadBeforeAnyWriterOpens() throws Exception {
        Path file = dir.resolve("race.db");
        try (CacheStore initial = CacheStore.open(file, Optional.of("A"))) {
            assertEquals("A", initial.clusterId());
        }
        Process[] child = new Process[1];
        byte[][] before = new byte[2][];
        CacheStore.beforeImmutableInspect = () -> {
            try {
                child[0] = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-cp", System.getProperty("java.class.path"), CacheWalProcess.class.getName(), file.toString())
                        .redirectError(ProcessBuilder.Redirect.INHERIT).start();
                var ready = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    try { return child[0].inputReader().readLine(); }
                    catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                }).get(10, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals("WAL", ready);
                before[0] = sha256(file); before[1] = sha256(Path.of(file + "-wal"));
            } catch (Exception e) { throw new SQLException(e); }
        };
        try {
            try (CacheStore concurrent = CacheStore.open(file, Optional.of("B"))) {
                assertEquals("B", concurrent.clusterId(), "the committed WAL supersedes the immutable main file");
            }
            SQLException refusal = assertThrows(SQLException.class, () -> CacheStore.open(file, Optional.of("A")));
            assertTrue(refusal.getMessage().contains("belongs to cluster B"), refusal.getMessage());
            assertArrayEquals(before[0], sha256(file));
            assertArrayEquals(before[1], sha256(Path.of(file + "-wal")));
        } finally {
            CacheStore.beforeImmutableInspect = () -> {};
            if (child[0] != null) {
                child[0].getOutputStream().write('\n'); child[0].getOutputStream().flush();
                assertTrue(child[0].waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(0, child[0].exitValue());
                child[0].destroyForcibly();
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    static List<String> query(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    row.append(i > 1 ? "|" : "").append(rs.getString(i));
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    private static Map<String, String> meta(Connection c) throws SQLException {
        Map<String, String> m = new TreeMap<>();
        for (String row : query(c, "SELECT name, value FROM \"$meta\"")) {
            String[] p = row.split("\\|", 2);
            m.put(p[0], p[1]);
        }
        return m;
    }

    private static byte[] sha256(Path f) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(f));
    }

    private List<String> files() throws Exception {
        try (Stream<Path> s = Files.walk(dir)) {
            return s.map(p -> dir.relativize(p).toString()).sorted().toList();
        }
    }

    private static Connection raw(Path f) throws SQLException {
        return new SQLiteConfig().createConnection("jdbc:sqlite:" + f);
    }

    private static boolean posix() {
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private static String perms(Path p) throws Exception {
        return PosixFilePermissions.toString(Files.getPosixFilePermissions(p));
    }

    // ------------------------------------------------------------------ a new cache

    @Test
    void aNewCacheHasEveryInternalTableAndItsMeta() throws Exception {
        Path f = dir.resolve("a/b/cache.db");
        long before = System.currentTimeMillis();
        try (CacheStore cache = CacheStore.open(f, A)) {
            assertEquals(f, cache.path());
            assertEquals("cluster-A_1", cache.clusterId());
            assertEquals(InternalTables.NAMES.stream().sorted().toList(),
                    query(cache.writer(), "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"));
            Map<String, String> meta = meta(cache.writer());
            assertEquals("cluster-A_1", meta.get("cluster_id"));
            assertEquals("3", meta.get("cache_format_version"));
            assertTrue(Long.parseLong(meta.get("created_ms")) >= before);
            assertEquals(List.of("wal"), query(cache.writer(), "PRAGMA journal_mode"));
            assertEquals(List.of("1"), query(cache.writer(), "PRAGMA foreign_keys"));
            assertEquals(List.of("5000"), query(cache.writer(), "PRAGMA busy_timeout"));
        }
    }

    @Test
    void permissionsOfANewFileAndDirectory() throws Exception {
        assumeTrue(posix());
        Path existing = Files.createDirectory(dir.resolve("home"),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
        Path f = existing.resolve(".kafcacheql/sub/cache.db");
        try (CacheStore cache = CacheStore.open(f, A); Statement s = cache.writer().createStatement()) {
            s.execute("INSERT INTO \"$subjects\" VALUES ('t-value', 1, '1', 0)"); // makes the -wal and -shm files
            assertEquals("rw-------", perms(f));
            assertEquals("rw-------", perms(Path.of(f + "-wal")));
            assertEquals("rw-------", perms(Path.of(f + "-shm")));
            assertEquals("rwx------", perms(f.getParent()));
            assertEquals("rwx------", perms(existing.resolve(".kafcacheql")));
            assertEquals("rwxr-xr-x", perms(existing), "a directory that already existed keeps its permissions");
        }
    }

    @Test
    void anEmptyFileLeftEarlierIsInitialised() throws Exception {
        Path f = Files.createFile(dir.resolve("empty.db"));
        try (CacheStore cache = CacheStore.open(f, A)) {
            assertEquals("cluster-A_1", meta(cache.writer()).get("cluster_id"));
        }
    }

    // ------------------------------------------------------------------ readers

    @Test
    void aReaderCantWrite() throws Exception {
        try (CacheStore cache = CacheStore.open(dir.resolve("c.db"), A); Connection r = cache.newReader();
             Statement s = r.createStatement()) {
            assertEquals(List.of("1"), query(r, "PRAGMA query_only"));
            SQLException e = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO \"$meta\" VALUES ('x', 'y')"));
            assertTrue(e.getMessage().contains("readonly"), e.getMessage());
            assertThrows(SQLException.class, () -> s.execute("CREATE TABLE t (x)"));
            assertThrows(SQLException.class, () -> s.execute("DELETE FROM \"$meta\""));
        }
    }

    @Test
    void aReaderIsntBlockedWhileTheWriterHoldsATransaction() throws Exception {
        try (CacheStore cache = CacheStore.open(dir.resolve("c.db"), A); Connection r = cache.newReader()) {
            Connection w = cache.writer();
            w.setAutoCommit(false);
            try (Statement s = w.createStatement()) {
                s.execute("INSERT INTO \"$subjects\" VALUES ('t-value', 1, '1', 0)");

                long start = System.nanoTime();
                assertEquals(List.of("0"), query(r, "SELECT count(*) FROM \"$subjects\""), "the last committed state");
                long millis = (System.nanoTime() - start) / 1_000_000;
                assertTrue(millis < CacheStore.BUSY_TIMEOUT_MS / 5, "answered in " + millis + " ms, not waiting");

                w.commit();
            } finally {
                w.setAutoCommit(true);
            }
            try (Connection fresh = cache.newReader()) {
                assertEquals(List.of("1"), query(fresh, "SELECT count(*) FROM \"$subjects\""));
            }
        }
    }

    // ------------------------------------------------------------------ reopening and refusing

    @Test
    void reopeningChangesNothing() throws Exception {
        Path f = dir.resolve("c.db");
        Map<String, String> first;
        try (CacheStore cache = CacheStore.open(f, A)) {
            first = meta(cache.writer());
        }
        byte[] bytes = sha256(f);
        List<String> listing = files();
        try (CacheStore cache = CacheStore.open(f, A)) {
            assertEquals(first, meta(cache.writer()));
        }
        assertArrayEquals(bytes, sha256(f));
        assertEquals(listing, files());
    }

    @Test
    void aCacheFromAnotherClusterIsRefusedAndLeftUntouched() throws Exception {
        Path f = dir.resolve("c.db");
        CacheStore.open(f, A).close();
        byte[] bytes = sha256(f);
        List<String> listing = files();

        SQLException e = assertThrows(SQLException.class, () -> CacheStore.open(f, B));
        assertTrue(e.getMessage().contains("belongs to cluster cluster-A_1, not cluster-B"), e.getMessage());
        assertEquals("08001", e.getSQLState());
        assertArrayEquals(bytes, sha256(f));
        assertEquals(listing, files());
    }

    @Test
    void anUnknownFormatVersionIsRefusedAndLeftUntouched() throws Exception {
        Path f = dir.resolve("c.db");
        CacheStore.open(f, A).close();
        try (Connection c = raw(f); Statement s = c.createStatement()) {
            s.execute("UPDATE \"$meta\" SET value = '99' WHERE name = 'cache_format_version'");
        }
        byte[] bytes = sha256(f);

        SQLException e = assertThrows(SQLException.class, () -> CacheStore.open(f, A));
        assertTrue(e.getMessage().contains("format version 99"), e.getMessage());
        assertArrayEquals(bytes, sha256(f));
    }

    @Test
    void aFileThatIsntACacheIsRefusedAndLeftUntouched() throws Exception {
        Path other = dir.resolve("other.db");
        try (Connection c = raw(other); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE customers (id INTEGER)");
        }
        byte[] otherBytes = sha256(other);
        assertTrue(assertThrows(SQLException.class, () -> CacheStore.open(other, A))
                .getMessage().contains("isn't a KafCacheQL cache"));
        assertArrayEquals(otherBytes, sha256(other));

        Path text = Files.writeString(dir.resolve("notes.db"), "not a database at all, just some text here");
        byte[] textBytes = sha256(text);
        assertTrue(assertThrows(SQLException.class, () -> CacheStore.open(text, A))
                .getMessage().contains("can't be read"));
        assertArrayEquals(textBytes, sha256(text));
    }

    @Test
    void aDamagedCacheIsRefusedAndLeftUntouched() throws Exception {
        for (String damage : List.of("DELETE FROM \"$meta\" WHERE name = 'cluster_id'",
                "UPDATE \"$meta\" SET value = '' WHERE name = 'cluster_id'", "DROP TABLE \"$subjects\"")) {
            Path f = Files.createTempDirectory(dir, "damaged").resolve("c.db");
            CacheStore.open(f, A).close();
            try (Connection c = raw(f); Statement s = c.createStatement()) {
                s.execute(damage);
            }
            byte[] bytes = sha256(f);
            for (Optional<String> cluster : List.of(A, Optional.<String>empty())) {
                SQLException e = assertThrows(SQLException.class, () -> CacheStore.open(f, cluster), damage);
                assertTrue(e.getMessage().contains("is damaged"), e.getMessage());
            }
            assertArrayEquals(bytes, sha256(f), damage);
        }
    }

    /** A process that stopped mid-way leaves committed data in the WAL; refusing that cache keeps it there. */
    @Test
    void refusingACacheLeftByACrashedProcessKeepsItsWal() throws Exception {
        Path f = dir.resolve("crashed.db");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), CacheCrash.class.getName(), f.toString())
                .redirectErrorStream(true).start();
        assertTrue(child.waitFor(60, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, child.exitValue(), new String(child.getInputStream().readAllBytes()));
        Path wal = Path.of(f + "-wal");
        assertTrue(Files.size(wal) > 0, "the committed data is in the WAL");
        byte[] main = sha256(f);
        byte[] walBytes = sha256(wal);

        SQLException e = assertThrows(SQLException.class, () -> CacheStore.open(f, B));
        assertTrue(e.getMessage().contains("belongs to cluster cluster-A, not cluster-B")
                || e.getMessage().contains("belongs to cluster A, not cluster-B"), e.getMessage());
        assertArrayEquals(main, sha256(f), "main file unchanged");
        assertArrayEquals(walBytes, sha256(wal), "WAL kept, unchanged");

        try (CacheStore cache = CacheStore.open(f, Optional.of("A"))) {
            assertEquals(List.of("1"), query(cache.writer(), "SELECT count(*) FROM \"$subjects\""), "nothing lost");
        }
    }

    /** SQLite resolves links; so does the cache, or it would check and restrict the wrong -wal and -shm files. */
    @Test
    void aPathThroughASymbolicLinkIsTheRealFile() throws Exception {
        assumeTrue(posix());
        Path real = dir.resolve("real.db");
        CacheStore.open(real, A).close();
        Files.setPosixFilePermissions(real, PosixFilePermissions.fromString("rw-r--r--"));
        Path alias = Files.createSymbolicLink(dir.resolve("alias.db"), real);
        try (CacheStore cache = CacheStore.open(alias, A); Statement s = cache.writer().createStatement()) {
            s.execute("INSERT INTO \"$subjects\" VALUES ('t-value', 1, '1', 0)");
            assertEquals(real.toRealPath(), cache.path());
            for (Path p : List.of(real, Path.of(real + "-wal"), Path.of(real + "-shm"))) {
                assertEquals("rw-------", perms(p), p.toString());
            }
            assertTrue(Files.notExists(Path.of(alias + "-wal")), "no -wal beside the link");
        }
    }

    /** "link/../cache.db" is the cache.db beside the link's target, as the file system reads it. */
    @Test
    void aDotDotAfterALinkLeadsFromTheLinksTarget() throws Exception {
        assumeTrue(posix());
        Path target = Files.createDirectories(dir.resolve("actual/sub"));
        Path link = Files.createSymbolicLink(dir.resolve("link"), target);
        Path meant = dir.resolve("actual/cache.db");
        try (CacheStore cache = CacheStore.open(meant, A); Statement s = cache.writer().createStatement()) {
            s.execute("INSERT INTO \"$subjects\" VALUES ('t-value', 1, '1', 0)");
        }
        CacheStore.open(dir.resolve("cache.db"), A).close(); // what a lexical ".." would pick: same cluster, no rows
        try (CacheStore cache = CacheStore.open(link.resolve("../cache.db"), A)) {
            assertEquals(meant.toRealPath(), cache.path());
            assertEquals(List.of("1"), query(cache.writer(), "SELECT count(*) FROM \"$subjects\""));
        }
    }

    @Test
    void offlineThroughALinkReadsTheWalACrashedProcessLeft() throws Exception {
        Path f = crash(dir.resolve("crashed-linked.db"));
        Path alias = Files.createSymbolicLink(dir.resolve("crashed-alias.db"), f);
        try (CacheStore cache = CacheStore.open(alias, Optional.empty())) {
            assertEquals("A", cache.clusterId());
            assertEquals(List.of("1"), query(cache.writer(), "SELECT count(*) FROM \"$subjects\""));
        }
    }

    /**
     * The maintainer's decision: a refusal leaves the cache's data, the file and its WAL, as they were. Reading
     * a WAL whose -shm file is gone makes SQLite create that coordination file, which is accepted.
     */
    @Test
    void refusingACacheWhoseShmIsGoneKeepsItsData() throws Exception {
        Path f = crash(dir.resolve("no-shm.db"));
        Files.delete(Path.of(f + "-shm"));
        Path wal = Path.of(f + "-wal");
        byte[] main = sha256(f);
        byte[] walBytes = sha256(wal);
        SQLException e = assertThrows(SQLException.class, () -> CacheStore.open(f, B));
        assertTrue(e.getMessage().contains("belongs to cluster A, not cluster-B"), e.getMessage());
        assertArrayEquals(main, sha256(f));
        assertArrayEquals(walBytes, sha256(wal));
    }

    /** A cache for cluster A whose last commit is still in the WAL, as a crashed process leaves it. */
    private static Path crash(Path f) throws Exception {
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), CacheCrash.class.getName(), f.toString())
                .redirectErrorStream(true).start();
        assertTrue(child.waitFor(60, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, child.exitValue(), new String(child.getInputStream().readAllBytes()));
        assertTrue(Files.size(Path.of(f + "-wal")) > 0, "the committed data is in the WAL");
        return f;
    }

    @Test
    void aPathSqliteWouldReadAsOptionsIsRefused() throws Exception {
        Path f = dir.resolve("literal.db?journal_mode=MEMORY");
        SQLException e = assertThrows(SQLException.class, () -> CacheStore.open(f, A));
        assertTrue(e.getMessage().contains("contains '?'"), e.getMessage());
        assertTrue(Files.notExists(f) && Files.notExists(dir.resolve("literal.db")), "nothing created");
    }

    @Test
    void pathsWithOtherAwkwardCharactersWork() throws Exception {
        Path f = Files.createDirectories(dir.resolve("a 100% #sure")).resolve("c.db");
        try (CacheStore cache = CacheStore.open(f, A)) {
            assertEquals(f.toRealPath().toString(),
                    query(cache.writer(), "SELECT file FROM pragma_database_list WHERE name = 'main'").get(0));
        }
        try (CacheStore cache = CacheStore.open(f, Optional.empty())) {
            assertEquals("cluster-A_1", cache.clusterId());
        }
    }

    @Test
    void anExistingFileOrItsWalGetOwnerOnlyPermissions() throws Exception {
        assumeTrue(posix());
        Path empty = Files.createFile(dir.resolve("empty-0644.db"));
        Files.setPosixFilePermissions(empty, PosixFilePermissions.fromString("rw-r--r--"));
        CacheStore.open(empty, A).close();
        assertEquals("rw-------", perms(empty));

        Path open = dir.resolve("loose.db");
        try (CacheStore first = CacheStore.open(open, A); Statement s = first.writer().createStatement()) {
            s.execute("INSERT INTO \"$subjects\" VALUES ('t-value', 1, '1', 0)");
            for (Path p : List.of(open, Path.of(open + "-wal"), Path.of(open + "-shm"))) {
                Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rw-rw-r--"));
            }
            try (CacheStore second = CacheStore.open(open, A)) {
                assertEquals("cluster-A_1", second.clusterId());
                for (Path p : List.of(open, Path.of(open + "-wal"), Path.of(open + "-shm"))) {
                    assertEquals("rw-------", perms(p), p.toString());
                }
            }
        }
    }

    @Test
    void simultaneousFirstOpensInitialiseTheCacheOnce() throws Exception {
        Path f = dir.resolve("race.db");
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            List<java.util.concurrent.Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    try (CacheStore c = CacheStore.open(f, A)) {
                        return c.clusterId();
                    }
                }));
            }
            start.countDown();
            for (java.util.concurrent.Future<String> r : results) {
                assertEquals("cluster-A_1", r.get(60, java.util.concurrent.TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdown();
        }
        try (Connection c = raw(f)) {
            assertEquals(List.of("1"), query(c, "SELECT count(*) FROM \"$meta\" WHERE name = 'cluster_id'"));
        }
    }

    @Test
    void simultaneousFirstOpensForTwoClustersLetOneWin() throws Exception {
        Path f = dir.resolve("race2.db");
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        List<String> outcomes = new ArrayList<>();
        try {
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            List<java.util.concurrent.Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                Optional<String> cluster = i % 2 == 0 ? A : B;
                results.add(pool.submit(() -> {
                    start.await();
                    try (CacheStore c = CacheStore.open(f, cluster)) {
                        return "opened " + c.clusterId();
                    } catch (SQLException e) {
                        return e.getMessage().contains("belongs to cluster") ? "refused" : "error: " + e.getMessage();
                    }
                }));
            }
            start.countDown();
            for (java.util.concurrent.Future<String> r : results) {
                outcomes.add(r.get(60, java.util.concurrent.TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdown();
        }
        List<String> opened = outcomes.stream().filter(o -> o.startsWith("opened")).distinct().toList();
        assertEquals(1, opened.size(), outcomes.toString());
        assertEquals(4, outcomes.stream().filter(o -> o.equals(opened.get(0))).count(), outcomes.toString());
        assertEquals(4, outcomes.stream().filter(o -> o.equals("refused")).count(), outcomes.toString());
    }

    // ------------------------------------------------------------------ offline

    @Test
    void offlineOpensAnExistingCacheWithItsOwnClusterId() throws Exception {
        Path f = dir.resolve("c.db");
        CacheStore.open(f, A).close();
        try (CacheStore cache = CacheStore.open(f, Optional.empty())) {
            assertEquals("cluster-A_1", cache.clusterId());
        }
    }

    @Test
    void offlineNeedsAFilledCache() throws Exception {
        assertTrue(assertThrows(SQLException.class, () -> CacheStore.open(dir.resolve("missing.db"), Optional.empty()))
                .getMessage().contains("No cache at"));
        assertTrue(Files.notExists(dir.resolve("missing.db")), "nothing created");
        Path empty = Files.createFile(dir.resolve("empty.db"));
        assertTrue(assertThrows(SQLException.class, () -> CacheStore.open(empty, Optional.empty()))
                .getMessage().contains("is empty"));
    }

    // ------------------------------------------------------------------ the path

    @Test
    void theDefaultPathIsNamedByTheClusterId() throws Exception {
        Path home = Path.of("/home/someone");
        assertEquals(Path.of("/home/someone/.kafcacheql/MkU3OEVBNTcwNTJENDM2Qk.db"),
                CacheStore.defaultPath(home, "MkU3OEVBNTcwNTJENDM2Qk"));
        assertEquals(Path.of("/home/someone/.kafcacheql/lkc-abc123.db"), CacheStore.defaultPath(home, "lkc-abc123"));
        assertThrows(SQLException.class, () -> CacheStore.defaultPath(home, "../../etc/passwd"));
        assertThrows(SQLException.class, () -> CacheStore.defaultPath(home, ""));
    }

    @Test
    void anExplicitCachePathWins() throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", "/tmp/mine.db");
        assertEquals(Path.of("/tmp/mine.db"), CacheStore.resolve(DriverConfig.load("jdbc:kafcacheql://h:1", p), A));
        assertEquals(Path.of(System.getProperty("user.home"), ".kafcacheql", "cluster-A_1.db"),
                CacheStore.resolve(DriverConfig.load("jdbc:kafcacheql://h:1", null), A));
    }
}
