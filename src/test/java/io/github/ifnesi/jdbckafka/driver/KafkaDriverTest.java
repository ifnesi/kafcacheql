package io.github.ifnesi.jdbckafka.driver;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.jdbckafka.config.DriverProperty;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaDriverTest {

    private final KafkaDriver driver = new KafkaDriver();

    @Test
    void acceptsOnlyItsOwnUrls() throws SQLException {
        assertTrue(driver.acceptsURL("jdbc:kafka://localhost:9092"));
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
        assertTrue(assertThrows(SQLException.class, () -> driver.connect("jdbc:kafka://h", null))
                .getMessage().startsWith("Malformed"));
        assertTrue(assertThrows(SQLException.class, () -> driver.connect("jdbc:kafka://h:1?offline=maybe", null))
                .getMessage().contains("offline"));
    }

    @Test
    void validConfigurationReachesTheConnectionStep() {
        // Until connections are built.
        assertThrows(SQLFeatureNotSupportedException.class, () -> driver.connect("jdbc:kafka://h:1", null));
    }

    @Test
    void registeredWithDriverManager() throws SQLException {
        assertEquals(KafkaDriver.class, DriverManager.getDriver("jdbc:kafka://localhost:9092").getClass());
    }

    @Test
    void propertyInfoListsEveryDriverPropertyWithItsDefault() {
        DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:kafka://h:1", new Properties());
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
                "jdbc:kafka://h:1?client.id=from-url&sync.batch.records=9&config.file=/no/such/file", p);
        assertEquals("from-properties", byName(info, "client.id").value);
        assertEquals("9", byName(info, "sync.batch.records").value);
        assertEquals("/no/such/file", byName(info, "config.file").value, "listed, not read");
    }

    @Test
    void propertyInfoForAnUnreadableUrlShowsDefaults() {
        DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:kafka:nonsense", null);
        assertEquals("1h", byName(info, "append.default.window").value);
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
