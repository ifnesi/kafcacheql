package io.github.ifnesi.kafcacheql.cache;

import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.sqlite.jdbc4.JDBC4Connection;

/**
 * The SQLite cache file: one writer connection, owned by the sync worker, and read-only connections for
 * user queries. The file belongs to one Kafka cluster; a cache from another cluster, in a format this driver
 * doesn't know, or damaged, is refused and its data, the file and its WAL, left as they were. (When a crashed
 * process left a WAL without its -shm file, SQLite creates that coordination file while reading the WAL: the
 * maintainer's decision accepts this.)
 *
 * <p>The cache is always the real file: a path through symbolic links is resolved first, by the file system
 * (a ".." after a link leads from the link's target), so its WAL, its -shm file and their permissions are the
 * ones SQLite uses.
 *
 * <p>Connections are created directly, not through SQLite's java.sql.Driver, whose class registers itself
 * with DriverManager when loaded: only KafCacheQL registers there.
 */
public final class CacheStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CacheStore.class);

    /** The cache layout, including topic indexes; a cache in any other version is refused. */
    public static final int FORMAT_VERSION = 9;

    static final int BUSY_TIMEOUT_MS = 5_000;
    public static final long JOURNAL_SIZE_LIMIT = 64L * 1024 * 1024;

    /** Reusable free pages do not count; neither VACUUM nor a checkpoint is needed to measure usage. */
    public static long usedBytes(Connection reader) throws SQLException {
        try (var s = reader.createStatement(); var r = s.executeQuery("""
                SELECT (page_count-freelist_count)*page_size
                FROM pragma_page_count(),pragma_freelist_count(),pragma_page_size()
                """)) {
            r.next(); return r.getLong(1);
        }
    }
    private static final Pattern CLUSTER_ID = Pattern.compile("[A-Za-z0-9._-]{1,255}");
    private static final Set<PosixFilePermission> OWNER_DIR = PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> OWNER_FILE = PosixFilePermissions.fromString("rw-------");
    private static final List<String> SIDECARS = List.of("-wal", "-shm", "-journal");

    private enum Role { INSPECT, WRITER, READER }

    /** Test seam at the no-WAL observation; a second process may create one before the inspection opens. */
    static volatile Check beforeImmutableInspect = () -> {};

    /** What a cache file holds: its $meta rows and its tables; empty for an empty database. */
    private record Contents(Map<String, String> meta, Set<String> tables) {}

    private final Path path;
    private final String clusterId;
    private final Connection writer;

    private CacheStore(Path path, String clusterId, Connection writer) {
        this.path = path;
        this.clusterId = clusterId;
        this.writer = writer;
    }

    /** {@code cache.path}, or {@code ~/.kafcacheql/<cluster-id>.db}. */
    public static Path resolve(DriverConfig config, Optional<String> clusterId) throws SQLException {
        if (config.cachePath().isPresent()) {
            return config.cachePath().get();
        }
        return defaultPath(Path.of(System.getProperty("user.home")),
                clusterId.orElseThrow(() -> new IllegalStateException("the default cache path needs the cluster ID")));
    }

    static Path defaultPath(Path home, String clusterId) throws SQLException {
        if (!CLUSTER_ID.matcher(clusterId).matches()) {
            throw ConfigErrors.problem("The cluster ID '" + clusterId + "' can't name a cache file: set cache.path");
        }
        return home.resolve(".kafcacheql").resolve(clusterId + ".db");
    }

    /**
     * Opens the cache, creating it (and its directory) when missing.
     *
     * @param clusterId the cluster the connection talks to; empty when offline, in which case the cache must
     *     exist and its own cluster ID is used
     */
    public static CacheStore open(Path path, Optional<String> clusterId) throws SQLException {
        return open(path, clusterId, reader -> {});
    }

    @FunctionalInterface public interface Inspection { void check(Connection reader) throws SQLException; }

    public static CacheStore open(Path path, Optional<String> clusterId, Inspection inspection) throws SQLException {
        // Not normalised: removing ".." before the file system follows a link would name another file.
        Path file = canonicalPath(path, clusterId.isPresent());

        // Checked on a read-only connection before anything writes: refusing a cache leaves it as it was.
        Optional<Contents> found = contents(file, clusterId, inspection);
        if (found.isEmpty() && clusterId.isEmpty()) {
            throw ConfigErrors.problem("The cache " + file + " is empty; offline needs a cache that has been filled");
        }
        Connection writer = connect(file, Role.WRITER);
        try {
            verifyIdentity(writer, file);
            String recorded = initialiseOrCheck(writer, file, clusterId);
            restrictPermissions(file);
            return new CacheStore(file, recorded, writer);
        } catch (SQLException | RuntimeException e) {
            writer.close();
            throw e;
        }
    }

    public Path path() {
        return path;
    }

    public String clusterId() {
        return clusterId;
    }

    /** The one connection that writes: the sync worker's. Its transactions are IMMEDIATE. */
    public Connection writer() {
        return writer;
    }

    @FunctionalInterface
    public interface Write<T> {
        T run() throws SQLException;
    }

    @FunctionalInterface
    public interface Check { void run() throws SQLException; }

    /** Resolve before joining a cache context; also refuse aliases unsafe for SQLite's path-named WAL. */
    public static Path canonicalPath(Path path, boolean online) throws SQLException {
        Path requested = path.toAbsolutePath();
        refuseOptions(requested);
        createIfMissing(requested, online);
        Path file = real(requested);
        refuseOptions(file);
        try {
            if (file.getFileSystem().supportedFileAttributeViews().contains("unix")
                    && ((Number) Files.getAttribute(file, "unix:nlink")).longValue() > 1) {
                throw ConfigErrors.problem("The cache " + file + " has multiple hard links; SQLite requires one real path");
            }
        } catch (IOException e) {
            throw ConfigErrors.problem("The cache " + file + " hard links can't be checked", e);
        }
        return file;
    }

    /**
     * One explicit write transaction. In particular, a result containing a newly allocated generation
     * cannot escape before COMMIT succeeds. JDBC auto-commit stays on: commit() with it off would start
     * another IMMEDIATE transaction and hold the write lock into subsequent work.
     */
    public static <T> T writeTransaction(Connection writer, Write<T> work) throws SQLException {
        return writeTransaction(writer, null, work);
    }

    /** The worker uses a short busy_timeout and checks the execution between write-lock attempts.
     * Only lock acquisition is repeated; transaction work is never replayed.
     */
    public static <T> T writeTransaction(Connection writer, Check check, Write<T> work) throws SQLException {
        if (!writer.getAutoCommit()) {
            throw new IllegalStateException("Cache writes require JDBC auto-commit to stay on");
        }
        try (Statement s = writer.createStatement()) {
            while (true) {
                if (check != null) check.run();
                try {
                    s.execute("BEGIN IMMEDIATE");
                    break;
                } catch (SQLiteException busy) {
                    if (check == null || busy.getResultCode() != SQLiteErrorCode.SQLITE_BUSY) throw busy;
                }
            }
            try {
                if (check != null) check.run();
                T result = work.run();
                if (check != null) check.run();
                s.execute("COMMIT");
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                try {
                    s.execute("ROLLBACK");
                } catch (SQLException rollback) {
                    e.addSuppressed(rollback);
                }
                throw e;
            }
        }
    }

    /** A new connection for user queries: {@code PRAGMA query_only = ON}, never blocked by the writer. */
    public Connection newReader() throws SQLException {
        return connect(path, Role.READER);
    }

    @Override
    public void close() throws SQLException {
        writer.close();
    }

    // ------------------------------------------------------------------ creating and checking

    private static void refuseOptions(Path file) throws SQLException {
        if (file.toString().contains("?")) {
            throw ConfigErrors.problem("The cache path " + file + " contains '?', which SQLite would read as the "
                    + "start of options: set cache.path to another file");
        }
    }

    /** The file itself, symbolic links resolved. */
    private static Path real(Path file) throws SQLException {
        try {
            return file.toRealPath();
        } catch (IOException e) {
            throw ConfigErrors.problem("The cache path " + file + " can't be resolved: " + e.getClass().getSimpleName());
        }
    }

    /** Creates the directory (0700) and an empty file (0600) when missing. */
    private static void createIfMissing(Path file, boolean online) throws SQLException {
        if (Files.exists(file)) {
            return;
        }
        if (!online) {
            throw ConfigErrors.problem("No cache at " + file + ": offline needs an existing cache");
        }
        boolean posix = posix();
        try {
            Path dir = file.getParent();
            if (dir != null && !Files.exists(dir)) {
                if (posix) {
                    Files.createDirectories(dir, attribute(OWNER_DIR));
                } else {
                    Files.createDirectories(dir);
                }
            }
            if (posix) {
                Files.createFile(file, attribute(OWNER_FILE));
            } else {
                Files.createFile(file);
            }
        } catch (FileAlreadyExistsException e) {
            // another connection created it first
        } catch (IOException e) {
            throw ConfigErrors.problem("The cache " + file + " can't be created: " + e.getClass().getSimpleName()
                    + (e.getMessage() == null ? "" : ": " + e.getMessage()));
        }
    }

    /**
     * What the file holds, read without writing (committed WAL content included); empty for an empty file.
     * Another connection may be creating the same new file at that moment: a read that fails is tried once
     * more before the cache is refused. The writer checks again, under the write lock, before using it.
     */
    private static Optional<Contents> contents(Path file, Optional<String> clusterId, Inspection inspection) throws SQLException {
        SQLException failure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            boolean hadWal = Files.exists(Path.of(file + SIDECARS.get(0)));
            try (Connection c = connect(file, Role.INSPECT, hadWal)) {
                Optional<Contents> found = contents(c, file);
                // Immutable readers ignore a WAL. Check after reading too: it may have appeared since
                // connect's existence check. A WAL-aware reader sees the committing writer's snapshot.
                if (!hadWal && Files.exists(Path.of(file + SIDECARS.get(0)))) {
                    try (Connection wal = connect(file, Role.INSPECT, true)) {
                        var current = contents(wal, file);
                        if (current.isPresent()) { check(file, current.get(), clusterId); inspection.check(wal); }
                        return current;
                    }
                }
                if (found.isPresent()) { check(file, found.get(), clusterId); inspection.check(c); }
                return found;
            } catch (SQLException e) {
                if (ConfigErrors.SQLSTATE.equals(e.getSQLState())) {
                    throw e;
                }
                failure = e;
                pause();
            }
        }
        throw ConfigErrors.problem("The cache " + file + " can't be read without writing to it: " + failure.getMessage(),
                failure);
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Optional<Contents> contents(Connection c, Path file) throws SQLException {
        Set<String> tables = new HashSet<>();
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT name FROM sqlite_master WHERE type = 'table'")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        if (tables.isEmpty()) {
            return Optional.empty();
        }
        if (!tables.contains(InternalTables.META)) {
            throw ConfigErrors.problem(file + " isn't a KafCacheQL cache: set cache.path to another file");
        }
        Map<String, String> meta = new HashMap<>();
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, value FROM \"$meta\"")) {
            while (rs.next()) {
                meta.put(rs.getString(1), rs.getString(2));
            }
        }
        return Optional.of(new Contents(meta, tables));
    }

    /** The cache's cluster ID, after checking its format, that it is whole, and that it belongs to the cluster. */
    private static String check(Path file, Contents contents, Optional<String> clusterId) throws SQLException {
        String version = contents.meta().get("cache_format_version");
        if (!String.valueOf(FORMAT_VERSION).equals(version)) {
            throw ConfigErrors.problem("The cache " + file + " has format version " + version + "; this driver "
                    + "reads version " + FORMAT_VERSION + ". Delete the file to rebuild it, or set cache.path");
        }
        String recorded = contents.meta().get("cluster_id");
        boolean whole = recorded != null && CLUSTER_ID.matcher(recorded).matches()
                && contents.tables().containsAll(InternalTables.NAMES);
        if (!whole) {
            throw ConfigErrors.problem("The cache " + file + " is damaged (its cluster ID or internal tables are "
                    + "missing): delete the file to rebuild it, or set cache.path");
        }
        if (clusterId.isPresent() && !clusterId.get().equals(recorded)) {
            throw ConfigErrors.problem("The cache " + file + " belongs to cluster " + recorded + ", not "
                    + clusterId.get() + ": set cache.path to another file, or delete this one");
        }
        return recorded;
    }

    /**
     * Under the writer's IMMEDIATE transaction, which holds the database's write lock: creates the internal
     * tables of an empty cache, or checks what another connection created since the read-only look. Opens of
     * one new file, in this JVM or others, initialise it once.
     */
    private static String initialiseOrCheck(Connection writer, Path file, Optional<String> clusterId)
            throws SQLException {
        return writeTransaction(writer, () -> {
            Optional<Contents> now = contents(writer, file);
            String recorded;
            if (now.isPresent()) {
                recorded = check(file, now.get(), clusterId);
            } else {
                if (clusterId.isEmpty()) {
                    throw ConfigErrors.problem("The cache " + file + " is empty; offline needs a cache that has been filled");
                }
                recorded = clusterId.get();
                try (Statement s = writer.createStatement()) {
                    for (String ddl : InternalTables.DDL) {
                        s.execute(ddl);
                    }
                }
                try (PreparedStatement p = writer.prepareStatement("INSERT INTO \"$meta\" (name, value) VALUES (?, ?)")) {
                    insert(p, "cluster_id", recorded);
                    insert(p, "cache_format_version", String.valueOf(FORMAT_VERSION));
                    insert(p, "created_ms", String.valueOf(System.currentTimeMillis()));
                }
            }
            return recorded;
        });
    }

    private static void insert(PreparedStatement p, String name, String value) throws SQLException {
        p.setString(1, name);
        p.setString(2, value);
        p.executeUpdate();
    }

    /** The file SQLite opened is the one asked for: a path must never be read as anything else. */
    private static void verifyIdentity(Connection writer, Path file) throws SQLException {
        String opened;
        try (Statement s = writer.createStatement();
             ResultSet rs = s.executeQuery("SELECT file FROM pragma_database_list WHERE name = 'main'")) {
            opened = rs.next() ? rs.getString(1) : null;
        }
        try {
            if (opened == null || !Path.of(opened).toRealPath().equals(file.toRealPath())) {
                throw ConfigErrors.problem("SQLite opened " + opened + " instead of the cache " + file
                        + ": set cache.path to another file");
            }
        } catch (IOException e) {
            throw ConfigErrors.problem("The cache " + file + " can't be checked: " + e.getClass().getSimpleName());
        }
    }

    /** The cache and its WAL, shared-memory and journal files: owner-only, whoever created them. */
    private static void restrictPermissions(Path file) throws SQLException {
        if (!posix()) {
            return;
        }
        try {
            for (Path p : List.of(file, Path.of(file + SIDECARS.get(0)), Path.of(file + SIDECARS.get(1)),
                    Path.of(file + SIDECARS.get(2)))) {
                if (Files.exists(p) && !Files.getPosixFilePermissions(p).equals(OWNER_FILE)) {
                    Files.setPosixFilePermissions(p, OWNER_FILE);
                    log.info("Restricted {} to its owner (rw-------)", p);
                }
            }
        } catch (IOException e) {
            throw ConfigErrors.problem("The permissions of the cache " + file + " can't be restricted to its owner: "
                    + e.getClass().getSimpleName());
        }
    }

    // ------------------------------------------------------------------ connections

    private static Connection connect(Path file, Role role) throws SQLException {
        return connect(file, role, false);
    }

    private static Connection connect(Path file, Role role, boolean walAware) throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(BUSY_TIMEOUT_MS);
        switch (role) {
            case INSPECT -> config.setReadOnly(true);
            case WRITER -> {
                // WAL is switched on below, once the busy timeout is in force: as an open-time setting it gives
                // up at once when another connection is switching the same new file.
                config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL); // the cache can always be rebuilt from Kafka
                config.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE);
                config.enforceForeignKeys(true);
            }
            case READER -> config.enforceForeignKeys(true);
        }
        String name = file.toString();
        boolean immutable = role == Role.INSPECT && !walAware && !Files.exists(Path.of(file + SIDECARS.get(0)));
        if (immutable) {
            beforeImmutableInspect.run();
            // Without a WAL, everything is in the main file: read it immutable, which creates no -wal or -shm
            // file. With a WAL, its files exist already, and a read-only connection reads what it holds.
            name = "file:" + name.replace("%", "%25").replace("#", "%23") + "?immutable=1";
        }
        Connection c = new JDBC4Connection("jdbc:sqlite:" + name, name, config.toProperties());
        try {
            if (role == Role.WRITER) {
                enableWal(c, file);
                try (Statement s = c.createStatement()) {
                    s.execute("PRAGMA journal_size_limit = " + JOURNAL_SIZE_LIMIT);
                }
            } else if (role == Role.READER) {
                try (Statement s = c.createStatement()) {
                    s.execute("PRAGMA query_only = ON");
                }
            }
        } catch (SQLException e) {
            c.close();
            throw e;
        }
        return c;
    }

    /**
     * Switches the file to WAL. While other connections switch the same new file, SQLite answers "busy" at
     * once instead of waiting out the busy timeout, so the switch is retried for that long.
     */
    private static void enableWal(Connection c, Path file) throws SQLException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(BUSY_TIMEOUT_MS);
        while (true) {
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("PRAGMA journal_mode = WAL")) {
                String mode = rs.next() ? rs.getString(1) : null;
                if (!"wal".equalsIgnoreCase(mode)) {
                    throw ConfigErrors.problem("The cache " + file + " can't use WAL (SQLite kept " + mode + ")");
                }
                return;
            } catch (SQLiteException e) {
                if (e.getResultCode() != SQLiteErrorCode.SQLITE_BUSY || System.nanoTime() > deadline) {
                    throw e;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private static boolean posix() {
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private static FileAttribute<Set<PosixFilePermission>> attribute(Set<PosixFilePermission> permissions) {
        return PosixFilePermissions.asFileAttribute(permissions);
    }
}
