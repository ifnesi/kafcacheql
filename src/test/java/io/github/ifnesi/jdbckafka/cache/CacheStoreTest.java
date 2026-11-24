package io.github.ifnesi.jdbckafka.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.ifnesi.jdbckafka.config.DriverConfig;
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

    // ------------------------------------------------------------------ helpers

    private static List<String> query(Connection c, String sql) throws SQLException {
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
            assertEquals("1", meta.get("cache_format_version"));
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
        Path f = existing.resolve(".jdbc-kafka/sub/cache.db");
        try (CacheStore cache = CacheStore.open(f, A); Statement s = cache.writer().createStatement()) {
            s.execute("INSERT INTO \"$subjects\" VALUES ('t-value', 1, '1', 0)"); // makes the -wal and -shm files
            assertEquals("rw-------", perms(f));
            assertEquals("rw-------", perms(Path.of(f + "-wal")));
            assertEquals("rw-------", perms(Path.of(f + "-shm")));
            assertEquals("rwx------", perms(f.getParent()));
            assertEquals("rwx------", perms(existing.resolve(".jdbc-kafka")));
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
                .getMessage().contains("isn't a jdbc-kafka cache"));
        assertArrayEquals(otherBytes, sha256(other));

        Path text = Files.writeString(dir.resolve("notes.db"), "not a database at all, just some text here");
        byte[] textBytes = sha256(text);
        assertTrue(assertThrows(SQLException.class, () -> CacheStore.open(text, A))
                .getMessage().contains("can't be read"));
        assertArrayEquals(textBytes, sha256(text));
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
        assertEquals(Path.of("/home/someone/.jdbc-kafka/MkU3OEVBNTcwNTJENDM2Qk.db"),
                CacheStore.defaultPath(home, "MkU3OEVBNTcwNTJENDM2Qk"));
        assertEquals(Path.of("/home/someone/.jdbc-kafka/lkc-abc123.db"), CacheStore.defaultPath(home, "lkc-abc123"));
        assertThrows(SQLException.class, () -> CacheStore.defaultPath(home, "../../etc/passwd"));
        assertThrows(SQLException.class, () -> CacheStore.defaultPath(home, ""));
    }

    @Test
    void anExplicitCachePathWins() throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", "/tmp/mine.db");
        assertEquals(Path.of("/tmp/mine.db"), CacheStore.resolve(DriverConfig.load("jdbc:kafka://h:1", p), A));
        assertEquals(Path.of(System.getProperty("user.home"), ".jdbc-kafka", "cluster-A_1.db"),
                CacheStore.resolve(DriverConfig.load("jdbc:kafka://h:1", null), A));
    }
}
