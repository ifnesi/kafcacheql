package io.github.ifnesi.jdbckafka.cache;

import io.github.ifnesi.jdbckafka.config.ConfigErrors;
import io.github.ifnesi.jdbckafka.config.DriverConfig;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.sqlite.SQLiteConfig;

/**
 * The SQLite cache file: one writer connection, owned by the sync worker, and
 * read-only connections for user queries. The file belongs to one Kafka cluster; a cache from another
 * cluster, or in a format this driver doesn't know, is refused and left untouched.
 */
public final class CacheStore implements AutoCloseable {

    /** The layout of the internal tables; a cache in any other version is refused. */
    public static final int FORMAT_VERSION = 1;

    static final int BUSY_TIMEOUT_MS = 5_000;
    private static final Pattern CLUSTER_ID = Pattern.compile("[A-Za-z0-9._-]{1,255}");
    private static final Set<PosixFilePermission> OWNER_DIR = PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> OWNER_FILE = PosixFilePermissions.fromString("rw-------");

    private final Path path;
    private final String clusterId;
    private final Connection writer;

    private CacheStore(Path path, String clusterId, Connection writer) {
        this.path = path;
        this.clusterId = clusterId;
        this.writer = writer;
    }

    /** {@code cache.path}, or {@code ~/.jdbc-kafka/<cluster-id>.db}. */
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
        return home.resolve(".jdbc-kafka").resolve(clusterId + ".db");
    }

    /**
     * Opens the cache, creating it (and its directory) when missing.
     *
     * @param clusterId the cluster the connection talks to; empty when offline, in which case the cache must
     *     exist and its own cluster ID is used
     */
    public static CacheStore open(Path path, Optional<String> clusterId) throws SQLException {
        Path file = path.toAbsolutePath().normalize();
        boolean created = createIfMissing(file, clusterId.isPresent());

        String recorded;
        if (created) {
            recorded = clusterId.orElseThrow();
        } else {
            // Checked before anything writes: refusing a cache must leave it as it was.
            Map<String, String> meta = readMeta(file);
            if (meta.isEmpty()) {
                if (clusterId.isEmpty()) {
                    throw ConfigErrors.problem("The cache " + file + " is empty; offline needs a cache that has been filled");
                }
                created = true; // created earlier but never initialised (an empty file is an empty database)
                recorded = clusterId.get();
            } else {
                recorded = check(file, meta, clusterId);
            }
        }

        Connection writer = connect(file, false);
        try {
            if (created) {
                initialise(writer, recorded);
            }
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

    /** The one connection that writes: the sync worker's. */
    public Connection writer() {
        return writer;
    }

    /** A new connection for user queries: {@code PRAGMA query_only = ON}, never blocked by the writer. */
    public Connection newReader() throws SQLException {
        return connect(path, true);
    }

    @Override
    public void close() throws SQLException {
        writer.close();
    }

    // ------------------------------------------------------------------

    /** Creates the directory (0700) and an empty file (0600) when missing; returns whether it did. */
    private static boolean createIfMissing(Path file, boolean online) throws SQLException {
        if (Files.exists(file)) {
            return false;
        }
        if (!online) {
            throw ConfigErrors.problem("No cache at " + file + ": offline needs an existing cache");
        }
        boolean posix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
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
            return true;
        } catch (FileAlreadyExistsException e) {
            return false; // another connection created it first
        } catch (IOException e) {
            throw ConfigErrors.problem("The cache " + file + " can't be created: " + e, e);
        }
    }

    private static FileAttribute<Set<PosixFilePermission>> attribute(Set<PosixFilePermission> permissions) {
        return PosixFilePermissions.asFileAttribute(permissions);
    }

    /** $meta's rows, read without writing; empty for an empty file; fails for a file that isn't a cache. */
    private static Map<String, String> readMeta(Path file) throws SQLException {
        try (Connection c = new SQLiteConfig().createConnection("jdbc:sqlite:" + file);
             Statement s = c.createStatement()) {
            s.execute("PRAGMA query_only = ON");
            boolean hasMeta;
            int tables;
            try (ResultSet rs = s.executeQuery("SELECT count(*), sum(name = '$meta') FROM sqlite_master WHERE type = 'table'")) {
                rs.next();
                tables = rs.getInt(1);
                hasMeta = rs.getInt(2) == 1;
            }
            if (tables == 0) {
                return Map.of();
            }
            if (!hasMeta) {
                throw ConfigErrors.problem(file + " isn't a jdbc-kafka cache: set cache.path to another file");
            }
            Map<String, String> meta = new HashMap<>();
            try (ResultSet rs = s.executeQuery("SELECT name, value FROM \"$meta\"")) {
                while (rs.next()) {
                    meta.put(rs.getString(1), rs.getString(2));
                }
            }
            return meta;
        } catch (SQLException e) {
            if (e.getSQLState() != null && e.getSQLState().startsWith("08")) {
                throw e;
            }
            throw ConfigErrors.problem("The cache " + file + " can't be read: " + e.getMessage(), e);
        }
    }

    /** The cache's cluster ID, after checking its format and that it belongs to the cluster. */
    private static String check(Path file, Map<String, String> meta, Optional<String> clusterId) throws SQLException {
        String version = meta.get("cache_format_version");
        if (!String.valueOf(FORMAT_VERSION).equals(version)) {
            throw ConfigErrors.problem("The cache " + file + " has format version " + version + "; this driver "
                    + "reads version " + FORMAT_VERSION + ". Delete the file to rebuild it, or set cache.path");
        }
        String recorded = meta.get("cluster_id");
        if (clusterId.isPresent() && !clusterId.get().equals(recorded)) {
            throw ConfigErrors.problem("The cache " + file + " belongs to cluster " + recorded + ", not "
                    + clusterId.get() + ": set cache.path to another file, or delete this one");
        }
        return recorded;
    }

    private static void initialise(Connection writer, String clusterId) throws SQLException {
        boolean autoCommit = writer.getAutoCommit();
        writer.setAutoCommit(false);
        try (Statement s = writer.createStatement()) {
            for (String ddl : InternalTables.DDL) {
                s.execute(ddl);
            }
            try (PreparedStatement p = writer.prepareStatement("INSERT INTO \"$meta\" (name, value) VALUES (?, ?)")) {
                insert(p, "cluster_id", clusterId);
                insert(p, "cache_format_version", String.valueOf(FORMAT_VERSION));
                insert(p, "created_ms", String.valueOf(System.currentTimeMillis()));
            }
            writer.commit();
        } catch (SQLException e) {
            writer.rollback();
            throw e;
        } finally {
            writer.setAutoCommit(autoCommit);
        }
    }

    private static void insert(PreparedStatement p, String name, String value) throws SQLException {
        p.setString(1, name);
        p.setString(2, value);
        p.executeUpdate();
    }

    private static Connection connect(Path file, boolean reader) throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL); // the cache is disposable: it can always be rebuilt from Kafka
        config.setBusyTimeout(BUSY_TIMEOUT_MS);
        config.enforceForeignKeys(true);
        Connection c = config.createConnection("jdbc:sqlite:" + file);
        if (reader) {
            try (Statement s = c.createStatement()) {
                s.execute("PRAGMA query_only = ON");
            } catch (SQLException e) {
                c.close();
                throw e;
            }
        }
        return c;
    }
}
