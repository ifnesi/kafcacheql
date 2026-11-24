package io.github.ifnesi.jdbckafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JdbcUrlTest {

    @Test
    void oneHost() throws SQLException {
        JdbcUrl url = JdbcUrl.parse("jdbc:kafka://localhost:9092");
        assertEquals(List.of("localhost:9092"), url.servers());
        assertEquals(Map.of(), url.params());
    }

    @Test
    void severalHostsIncludingIpv6AndDockerNames() throws SQLException {
        JdbcUrl url = JdbcUrl.parse("jdbc:kafka://broker-1:9092,10.0.0.2:9093,[::1]:9094,schema_registry.local:1");
        assertEquals(List.of("broker-1:9092", "10.0.0.2:9093", "[::1]:9094", "schema_registry.local:1"), url.servers());
    }

    @ParameterizedTest
    @ValueSource(strings = {"jdbc:kafka://", "jdbc:kafka:///", "jdbc:kafka://?config.file=/x", "jdbc:kafka:///?config.file=/x"})
    void noHosts(String s) throws SQLException {
        assertEquals(List.of(), JdbcUrl.parse(s).servers());
    }

    @Test
    void trailingSlashAndAmpersandsAreTolerated() throws SQLException {
        JdbcUrl url = JdbcUrl.parse("jdbc:kafka://h:1/?&a=1&&b=2&");
        assertEquals(List.of("h:1"), url.servers());
        assertEquals(Map.of("a", "1", "b", "2"), url.params());
    }

    @Test
    void parametersArePercentDecodedAsUtf8AndPlusStaysPlus() throws SQLException {
        JdbcUrl url = JdbcUrl.parse("jdbc:kafka://h:1?topics.exclude=__.*%7C_confluent.%2B&k%3Dey=v%26alue%3D"
                + "&empty=&utf=caf%C3%A9%F0%9F%98%80&plus=a+b&eq=x=y");
        assertEquals("__.*|_confluent.+", url.params().get("topics.exclude"));
        assertEquals("v&alue=", url.params().get("k=ey"));
        assertEquals("", url.params().get("empty"));
        assertEquals("café😀", url.params().get("utf"));
        assertEquals("a+b", url.params().get("plus"));
        assertEquals("x=y", url.params().get("eq"), "only the first = splits");
    }

    @Test
    void unescapedNonAsciiIsKept() throws SQLException {
        assertEquals("café😀", JdbcUrl.parse("jdbc:kafka://h:1?k=café😀").params().get("k"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "jdbc:kafka:localhost:9092",          // no //
            "jdbc:kafka://localhost",             // no port
            "jdbc:kafka://localhost:",            // empty port
            "jdbc:kafka://localhost:0",           // port out of range
            "jdbc:kafka://localhost:65536",
            "jdbc:kafka://localhost:90x2",
            "jdbc:kafka://:9092",                 // empty host
            "jdbc:kafka://h:1,,h:2",              // empty entry
            "jdbc:kafka://h:1,",
            "jdbc:kafka://bad host:1",
            "jdbc:kafka://h:1/db",                // a path
            "jdbc:kafka://[::1]",                 // IPv6 without a port
            "jdbc:kafka://[zz]:1",
            "jdbc:kafka://h:1?novalue",           // key without =
            "jdbc:kafka://h:1?=v",                // empty key
            "jdbc:kafka://h:1?a=1&a=2",           // duplicate key
            "jdbc:kafka://h:1?a=%",               // incomplete escapes
            "jdbc:kafka://h:1?a=%4",
            "jdbc:kafka://h:1?a=%zz",             // not hex
            "jdbc:kafka://h:1?a=%C3",             // not UTF-8
            "jdbc:postgresql://h:1",
    })
    void malformedUrlsFail(String s) {
        SQLException e = assertThrows(SQLException.class, () -> JdbcUrl.parse(s));
        assertTrue(e.getMessage().startsWith("Malformed jdbc-kafka URL"), e.getMessage());
        assertEquals("08001", e.getSQLState());
    }

    @Test
    void errorsNeverEchoParameterValues() {
        SQLException e = assertThrows(SQLException.class,
                () -> JdbcUrl.parse("jdbc:kafka://h:1?sasl.jaas.config=secret-value&sasl.jaas.config=secret-value"));
        assertFalse(e.getMessage().contains("secret-value"), e.getMessage());
    }

    @Test
    void accepts() {
        assertTrue(JdbcUrl.accepts("jdbc:kafka://h:1"));
        assertTrue(JdbcUrl.accepts("jdbc:kafka:anything"), "accepted, then rejected as malformed by parse");
        assertFalse(JdbcUrl.accepts("jdbc:sqlite::memory:"));
        assertFalse(JdbcUrl.accepts(null));
    }
}
