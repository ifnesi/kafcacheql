package io.github.ifnesi.kafcacheql.driver;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.DriverProperty;
import io.github.ifnesi.kafcacheql.config.JdbcUrl;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The KafCacheQL driver. Registered with {@link DriverManager} when loaded, which JDBC 4
 * does through META-INF/services/java.sql.Driver.
 */
public final class KafCacheQLDriver implements Driver {

    public static final int MAJOR_VERSION = 0;
    public static final int MINOR_VERSION = 1;

    static {
        try {
            DriverManager.registerDriver(new KafCacheQLDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Returns null for URLs that aren't {@code jdbc:kafcacheql:}, so DriverManager can try other drivers. */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        // With the driver's class loader throughout (DriverLoader): tools often call from a thread whose
        // context class loader can't see the driver's classes.
        return DriverLoader.call(() -> Jdbc.connection(CacheContext.acquire(DriverConfig.load(url, info)), url));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("The URL is null");
        }
        return JdbcUrl.accepts(url);
    }

    /**
     * Every driver property, so tools such as DBeaver can list them, with the value the
     * Properties object or the URL gives it, else its default. Never fails: an unreadable URL only means its
     * parameters aren't shown. Client settings aren't listed; config.file isn't read.
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        Map<String, String> fromUrl = Map.of();
        try {
            fromUrl = JdbcUrl.parse(url).params();
        } catch (SQLException e) {
            // shown with defaults
        }
        List<DriverPropertyInfo> out = new ArrayList<>();
        for (DriverProperty p : DriverProperty.values()) {
            String value = info == null ? null : info.getProperty(p.key());
            if (value == null) {
                value = fromUrl.getOrDefault(p.key(), p.defaultValue());
            }
            DriverPropertyInfo i = new DriverPropertyInfo(p.key(), value != null && Secrets.isSecret(p.key()) ? "[redacted]" : value);
            i.description = p.description();
            i.required = false;
            i.choices = p.choices().length == 0 ? null : p.choices();
            out.add(i);
        }
        return out.toArray(DriverPropertyInfo[]::new);
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    /** Not JDBC-compliant: no SQL-92 entry level, no transactions, SELECT only. */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /** The driver logs through SLF4J, not java.util.logging. */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("KafCacheQL logs through SLF4J");
    }
}
