package io.github.ifnesi.kafcacheql.driver;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.config.DriverProperty;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafCacheQLDriverTest {

    private final KafCacheQLDriver driver = new KafCacheQLDriver();

    @Test
    void acceptsOnlyItsOwnUrls() throws SQLException {
        assertTrue(driver.acceptsURL("jdbc:kafcacheql://localhost:9092"));
        assertFalse(driver.acceptsURL("jdbc:sqlite::memory:"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://h/db"));
        assertThrows(SQLException.class, () -> driver.acceptsURL(null));
    }

    @Test
    void foreignUrlsGiveNull() throws SQLException {
        assertNull(driver.connect("jdbc:sqlite::memory:", new Properties()));
        assertNull(driver.connect("jdbc:postgresql://h/db", null));
    }

    @Test
    void nullUrlFails() {
        assertThrows(SQLException.class, () -> driver.connect(null, new Properties()));
    }

    @Test
    void malformedUrlsAndInvalidConfigurationFailConnect() {
        assertTrue(assertThrows(SQLException.class, () -> driver.connect("jdbc:kafcacheql://h", null))
                .getMessage().startsWith("Malformed"));
        assertTrue(assertThrows(SQLException.class, () -> driver.connect("jdbc:kafcacheql://h:1?offline=maybe", null))
                .getMessage().contains("offline"));
    }

    /** Invalid input is always an SQLException with SQLState 08001, never an unchecked exception. */
    @Test
    void everyConfigurationErrorIsAnSqlException() throws Exception {
        java.nio.file.Path bad = java.nio.file.Files.createTempFile("bad", ".properties");
        java.nio.file.Files.writeString(bad, "a=\\uXXXX\n");
        Properties p = new Properties();
        p.setProperty("config.file", bad.toString());
        for (String url : java.util.List.of("jdbc:kafcacheql://h:1?cache.path=%00", "jdbc:kafcacheql://h:1?config.file=%00",
                "jdbc:kafcacheql://h:1?topic...mode=append", "jdbc:kafcacheql://u:p@h:1",
                "jdbc:kafcacheql://h:1?sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule%20required%20password=a%20b;")) {
            SQLException e = assertThrows(SQLException.class, () -> driver.connect(url, null), url);
            assertEquals("08001", e.getSQLState(), url);
        }
        assertEquals("08001", assertThrows(SQLException.class, () -> driver.connect("jdbc:kafcacheql://h:1", p)).getSQLState());
        java.nio.file.Files.delete(bad);
    }

    @Test
    void validConfigurationReachesTheConnectionStep() throws Exception {
        var file = java.nio.file.Files.createTempDirectory("driver-connect").resolve("cache.db");
        try (var cache = io.github.ifnesi.kafcacheql.cache.CacheStore.open(file, java.util.Optional.of("unit-cluster"))) {
            assertEquals("unit-cluster", cache.clusterId());
        }
        Properties p = new Properties();
        p.setProperty("offline", "true"); p.setProperty("cache.path", file.toString());
        try (var c = driver.connect("jdbc:kafcacheql://", p); var s = c.createStatement(); var rows = s.executeQuery("SELECT 1")) {
            assertTrue(rows.next()); assertEquals(1, rows.getInt(1)); assertFalse(rows.next());
        }
    }

    @Test
    void registeredWithDriverManager() throws SQLException {
        assertEquals(KafCacheQLDriver.class, DriverManager.getDriver("jdbc:kafcacheql://localhost:9092").getClass());
    }

    @Test
    void propertyInfoListsEveryDriverPropertyWithItsDefault() {
        DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:kafcacheql://h:1", new Properties());
        assertEquals(DriverProperty.values().length, info.length);
        for (int i = 0; i < info.length; i++) {
            DriverProperty p = DriverProperty.values()[i];
            assertEquals(p.key(), info[i].name);
            assertEquals(p.defaultValue(), info[i].value);
            assertEquals(p.description(), info[i].description);
            assertFalse(info[i].required);
        }
        DriverPropertyInfo offline = byName(info, "offline");
        assertArrayEquals(new String[] {"true", "false"}, offline.choices);
        assertNull(byName(info, "append.default.window").choices, "any duration");
    }

    @Test
    void propertyInfoShowsGivenValuesPropertiesFirst() {
        Properties p = new Properties();
        p.setProperty("client.id", "from-properties");
        DriverPropertyInfo[] info = driver.getPropertyInfo(
                "jdbc:kafcacheql://h:1?client.id=from-url&sync.batch.records=9&config.file=/no/such/file", p);
        assertEquals("from-properties", byName(info, "client.id").value);
        assertEquals("9", byName(info, "sync.batch.records").value);
        assertEquals("/no/such/file", byName(info, "config.file").value, "listed, not read");
    }

    @Test
    void propertyInfoForAnUnreadableUrlShowsDefaults() {
        DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:kafcacheql:nonsense", null);
        assertEquals("all", byName(info, "append.default.window").value);
    }

    private static DriverPropertyInfo byName(DriverPropertyInfo[] info, String name) {
        return java.util.Arrays.stream(info).filter(i -> i.name.equals(name)).findFirst().orElseThrow();
    }

    @Test
    void versionAndCompliance() {
        assertEquals(0, driver.getMajorVersion());
        assertEquals(1, driver.getMinorVersion());
        assertFalse(driver.jdbcCompliant());
        assertThrows(SQLFeatureNotSupportedException.class, driver::getParentLogger);
    }
}
