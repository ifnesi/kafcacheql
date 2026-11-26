package io.github.ifnesi.kafcacheql.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class DriverConfigTest {

    private static final String URL = "jdbc:kafcacheql://localhost:9092";

    @TempDir
    Path dir;

    private static Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    private Path file(String content) throws IOException {
        Path f = Files.createTempFile(dir, "config", ".properties");
        Files.writeString(f, content);
        return f;
    }

    private static SQLException fails(String url, Properties info) {
        SQLException e = assertThrows(SQLException.class, () -> DriverConfig.load(url, info));
        assertEquals("08001", e.getSQLState());
        return e;
    }

    @Test
    void defaultsAreThoseOfPlanSection43() throws SQLException {
        DriverConfig c = DriverConfig.load(URL, null);
        assertEquals(Optional.of("localhost:9092"), c.bootstrapServers());
        assertEquals(Optional.empty(), c.cachePath());
        assertTrue(c.cacheMaxBytes().isEmpty());
        assertTrue(c.cacheRetentionHours().isEmpty());
        assertEquals(Window.of(Duration.ofHours(1)), c.appendDefaultWindow());
        assertEquals(10_000, c.appendDefaultWindowMaxRecords());
        assertEquals(0, c.maxStalenessMs());
        assertFalse(c.offline());
        assertEquals(".*", c.topicsInclude().pattern());
        assertEquals("__.*|_confluent.*|_schemas", c.topicsExclude().pattern());
        assertFalse(c.metadataShowInternal());
        assertEquals("kafcacheql", c.clientId());
        assertEquals(5_000, c.syncBatchRecords());
        assertEquals(Map.of(), c.topics());
        assertEquals(Map.of("bootstrap.servers", "localhost:9092"), c.clientSettings());
    }

    @Nested
    class Precedence {

        private static final String KEY = "append.default.window";

        private Window window(String url, Properties info) throws SQLException {
            return DriverConfig.load(url, info).appendDefaultWindow();
        }

        @Test
        void configFileOverDefaults() throws Exception {
            Path f = file(KEY + "=2h\n");
            assertEquals(Window.of(Duration.ofHours(2)), window(URL + "?config.file=" + f, null));
        }

        @Test
        void urlOverConfigFile() throws Exception {
            Path f = file(KEY + "=2h\n");
            assertEquals(Window.of(Duration.ofHours(3)), window(URL + "?config.file=" + f + "&" + KEY + "=3h", null));
        }

        @Test
        void propertiesOverUrl() throws SQLException {
            assertEquals(Window.of(Duration.ofHours(4)), window(URL + "?" + KEY + "=3h", props(KEY, "4h")));
        }

        @Test
        void propertiesOverConfigFile() throws Exception {
            Path f = file(KEY + "=2h\n");
            assertEquals(Window.of(Duration.ofHours(4)), window(URL, props("config.file", f.toString(), KEY, "4h")));
        }

        @Test
        void urlOverDefaults() throws SQLException {
            assertEquals(Window.of(Duration.ofHours(3)), window(URL + "?" + KEY + "=3h", null));
        }

        @Test
        void propertiesOverDefaults() throws SQLException {
            assertEquals(Window.ALL, window(URL, props(KEY, "all")));
        }

        @Test
        void configFileNamedInPropertiesWinsOverOneNamedInTheUrl() throws Exception {
            Path inUrl = file(KEY + "=2h\n");
            Path inProps = file(KEY + "=5h\n");
            assertEquals(Window.of(Duration.ofHours(5)),
                    window(URL + "?config.file=" + inUrl, props("config.file", inProps.toString())));
        }

        @Test
        void nonStringPropertyValuesCount() throws SQLException {
            Properties p = new Properties();
            p.put("sync.batch.records", 42);
            assertEquals(42, DriverConfig.load(URL, p).syncBatchRecords());
        }
    }

    @Nested
    class ConfigFile {

        @Test
        void confluentCloudStyleFileFillsClientSettings() throws Exception {
            Path f = file("""
                    bootstrap.servers=pkc-xxxx.region.provider.confluent.cloud:9092
                    security.protocol=SASL_SSL
                    sasl.mechanisms=PLAIN
                    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='k' password='s';
                    schema.registry.url=https://psrc-xxxx.region.provider.confluent.cloud
                    basic.auth.credentials.source=USER_INFO
                    basic.auth.user.info=user:secret
                    """);
            DriverConfig c = DriverConfig.load("jdbc:kafcacheql://?config.file=" + f, null);
            assertEquals(Optional.of("pkc-xxxx.region.provider.confluent.cloud:9092"), c.bootstrapServers());
            assertEquals("SASL_SSL", c.clientSettings().get("security.protocol"));
            assertEquals("user:secret", c.clientSettings().get("basic.auth.user.info"));
            assertEquals(7, c.clientSettings().size());
        }

        @Test
        void missingFileFails() {
            fails(URL + "?config.file=" + dir.resolve("nope.properties"), null);
        }

        @Test
        void unreadableFileFails() throws Exception {
            assumeFalse("root".equals(System.getProperty("user.name")), "root reads anything");
            Path f = file("a=1\n");
            Files.setPosixFilePermissions(f, PosixFilePermissions.fromString("---------"));
            fails(URL + "?config.file=" + f, null);
        }

        @Test
        void configFileInsideTheConfigFileFails() throws Exception {
            Path inner = file("a=1\n");
            Path f = file("config.file=" + inner + "\n");
            assertTrue(fails(URL, props("config.file", f.toString())).getMessage().contains("sets config.file"));
        }

        @Test
        void aPathTheFileSystemCantHoldFailsAsConfiguration() {
            assertTrue(fails(URL + "?cache.path=%00", null).getMessage().contains("cache.path"));
            assertTrue(fails(URL + "?config.file=%00", null).getMessage().contains("config.file"));
        }

        @Test
        void aMalformedPropertiesFileFailsAsConfiguration() throws Exception {
            Path f = file("sasl.jaas.config=\\uXXXX\n");
            assertTrue(fails(URL, props("config.file", f.toString())).getMessage().contains("isn't a valid properties file"));
        }

        @Test
        void homeDirectoryIsExpanded() throws SQLException {
            assertEquals(Path.of(System.getProperty("user.home"), "x", "y.db"), DriverConfig.path("k", "~/x/y.db"));
            assertEquals(Path.of(System.getProperty("user.home")), DriverConfig.path("k", "~"));
            assertEquals(Path.of("/abs/c.db"), DriverConfig.path("k", "/abs/c.db"));
        }
    }

    @Nested
    class Bootstrap {

        @Test
        void fromConfigWhenTheUrlHasNoHosts() throws SQLException {
            DriverConfig c = DriverConfig.load("jdbc:kafcacheql://", props("bootstrap.servers", "a:1, b:2"));
            assertEquals(Optional.of("a:1, b:2"), c.bootstrapServers());
        }

        @Test
        void urlHostsAndTheSameSettingAgree() throws SQLException {
            DriverConfig c = DriverConfig.load("jdbc:kafcacheql://a:1,b:2", props("bootstrap.servers", "b:2,a:1"));
            assertEquals(Optional.of("a:1,b:2"), c.bootstrapServers());
            assertEquals("a:1,b:2", c.clientSettings().get("bootstrap.servers"));
        }

        @Test
        void urlHostsAndADifferentSettingConflict() {
            assertTrue(fails("jdbc:kafcacheql://a:1", props("bootstrap.servers", "c:3")).getMessage().contains("disagree"));
        }

        @Test
        void noneFails() {
            assertTrue(fails("jdbc:kafcacheql://", null).getMessage().contains("No bootstrap servers"));
        }

        @Test
        void offlineNeedsNoServersButNeedsACachePath() throws SQLException {
            assertTrue(fails("jdbc:kafcacheql://?offline=true", null).getMessage().contains("cache.path"));
            DriverConfig c = DriverConfig.load("jdbc:kafcacheql://?offline=true&cache.path=/tmp/x.db", null);
            assertEquals(Optional.empty(), c.bootstrapServers());
            assertEquals(Optional.of(Path.of("/tmp/x.db")), c.cachePath());
        }
    }

    @Nested
    class Values {

        @ParameterizedTest
        @CsvSource({"500ms,PT0.5S", "90s,PT1M30S", "15m,PT15M", "1h,PT1H", "2d,PT48H"})
        void durations(String value, String iso) throws SQLException {
            assertEquals(Window.of(Duration.parse(iso)),
                    DriverConfig.load(URL, props("append.default.window", value)).appendDefaultWindow());
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "0h", "0ms", "-1h", "1", "h", "1.5h", "1H", "1 h", "1w", "ALL", "99999999999999999d"})
        void badWindows(String value) {
            fails(URL, props("append.default.window", value));
        }

        @ParameterizedTest
        @CsvSource({
                "append.default.window.max.records,0", "append.default.window.max.records,-5",
                "append.default.window.max.records,ten", "max.staleness.ms,1.5",
                "sync.batch.records,0", "sync.batch.records,2147483648",
                "cache.max.bytes,0", "cache.max.bytes,-1", "cache.max.bytes,1.5", "cache.max.bytes,9223372036854775808",
                "cache.retention.hours,0", "cache.retention.hours,-1", "cache.retention.hours,1.5",
                "cache.retention.hours,2562047788016", "cache.retention.hours,9223372036854775808",
                "offline,yes", "metadata.show.internal,''",
                "topics.include,(", "topics.exclude,[a-", "client.id,' '"})
        void badValuesNameTheProperty(String key, String value) {
            SQLException e = fails(URL, props(key, value));
            assertTrue(e.getMessage().contains(key), e.getMessage());
        }

        @Test
        void goodValues() throws SQLException {
            DriverConfig c = DriverConfig.load(URL, props(
                    "append.default.window.max.records", "1",
                    "max.staleness.ms", "30000",
                    "offline", "FALSE",
                    "metadata.show.internal", "TRUE",
                    "topics.include", "orders\\..*",
                    "topics.exclude", "",
                    "client.id", "my-app",
                    "sync.batch.records", "2147483647",
                    "cache.max.bytes", "9223372036854775807",
                    "cache.retention.hours", "24",
                    "cache.path", "~/c.db"));
            assertEquals(1, c.appendDefaultWindowMaxRecords());
            assertEquals(30_000, c.maxStalenessMs());
            assertEquals(Long.MAX_VALUE, c.cacheMaxBytes().orElseThrow());
            assertEquals(24, c.cacheRetentionHours().orElseThrow());
            assertTrue(c.metadataShowInternal());
            assertTrue(c.topicsInclude().matcher("orders.v1").matches());
            assertEquals("", c.topicsExclude().pattern());
            assertEquals("my-app", c.clientId());
            assertEquals(Integer.MAX_VALUE, c.syncBatchRecords());
            assertEquals(Optional.of(Path.of(System.getProperty("user.home"), "c.db")), c.cachePath());
        }

        @Test
        void driverPropertiesDontReachTheClients() throws SQLException {
            DriverConfig c = DriverConfig.load(URL + "?client.id=x&sync.batch.records=7&topic.t.mode=append"
                    + "&cache.retention.hours=1&security.protocol=SSL", null);
            assertEquals(Map.of("bootstrap.servers", "localhost:9092", "security.protocol", "SSL"), c.clientSettings());
        }
    }

    @Nested
    class PerTopic {

        @Test
        void everySetting() throws SQLException {
            DriverConfig c = DriverConfig.load(URL, props(
                    "topic.orders.mode", "compacted",
                    "topic.orders.key.format", "string",
                    "topic.orders.value.format", "json-sr",
                    "topic.orders.append.default.window", "15m",
                    "topic.orders.append.default.window.max.records", "500",
                    "topic.orders.delete.retention.ms", "3600000"));
            TopicSettings t = c.topic("orders");
            assertEquals(Optional.of(Mode.COMPACTED), t.mode());
            assertEquals(Optional.of(Format.STRING), t.keyFormat());
            assertEquals(Optional.of(Format.JSON_SR), t.valueFormat());
            assertEquals(Optional.of(Window.of(Duration.ofMinutes(15))), t.appendDefaultWindow());
            assertEquals(OptionalLong.of(500), t.appendDefaultWindowMaxRecords());
            assertEquals(OptionalLong.of(3_600_000), t.deleteRetentionMs());
        }

        @Test
        void dottedTopicNames() throws SQLException {
            DriverConfig c = DriverConfig.load(URL, props(
                    "topic.payments.v1.append.default.window", "all",
                    "topic.a.b.c.mode", "append"));
            assertEquals(Optional.of(Window.ALL), c.topic("payments.v1").appendDefaultWindow());
            assertEquals(Optional.of(Mode.APPEND), c.topic("a.b.c").mode());
        }

        @Test
        void topicNamesThatEndWithASettingsSuffix() throws SQLException {
            DriverConfig c = DriverConfig.load(URL, props(
                    "topic.events.mode.mode", "compacted",
                    "topic.x.append.default.window.append.default.window.max.records", "9",
                    "topic.y.key.format.value.format", "bytes",
                    "topic.z.append.default.window", "2h"));
            assertEquals(Optional.of(Mode.COMPACTED), c.topic("events.mode").mode());
            assertEquals(OptionalLong.of(9), c.topic("x.append.default.window").appendDefaultWindowMaxRecords());
            assertEquals(Optional.of(Format.BYTES), c.topic("y.key.format").valueFormat());
            assertEquals(Optional.empty(), c.topic("y").valueFormat());
            assertEquals(Optional.of(Window.of(Duration.ofHours(2))), c.topic("z").appendDefaultWindow());
        }

        @Test
        void perTopicOverridesWinOverTheConnection() throws SQLException {
            DriverConfig c = DriverConfig.load(URL, props(
                    "append.default.window", "1h", "topic.t.append.default.window", "5m",
                    "append.default.window.max.records", "100", "topic.t.append.default.window.max.records", "7"));
            assertEquals(Window.of(Duration.ofMinutes(5)), c.appendDefaultWindow("t"));
            assertEquals(Window.of(Duration.ofHours(1)), c.appendDefaultWindow("other"));
            assertEquals(7, c.appendDefaultWindowMaxRecords("t"));
            assertEquals(100, c.appendDefaultWindowMaxRecords("other"));
            assertEquals(TopicSettings.NONE, c.topic("other"));
        }

        @ParameterizedTest
        @CsvSource({
                "topic.t.mode,stream", "topic.t.key.format,xml", "topic.t.value.format,AVRO",
                "topic.t.append.default.window,0m", "topic.t.append.default.window.max.records,0",
                "topic.t.delete.retention.ms,-1", "topic.t.colour,red", "topic.mode,append",
                "topic..mode,append", "topic.bad name.mode,append", "topic.bad/name.mode,append",
                "topic...mode,append", "topic....mode,append"})
        void badSettings(String key, String value) {
            SQLException e = fails(URL, props(key, value));
            assertTrue(e.getMessage().contains(key), e.getMessage());
        }

        /** Keeps topic.<name>.<setting> unambiguous: a key can then end with only one setting's suffix. */
        @Test
        void noSettingSuffixEndsWithAnother() {
            for (TopicSettings.Setting a : TopicSettings.Setting.values()) {
                for (TopicSettings.Setting b : TopicSettings.Setting.values()) {
                    if (a != b) {
                        assertFalse(("." + a.suffix).endsWith("." + b.suffix), a + " ends with " + b);
                    }
                }
            }
        }

        @Test
        void a249CharacterNameIsTheLongestAllowed() throws SQLException {
            String longest = "t".repeat(249);
            assertEquals(Optional.of(Mode.APPEND),
                    DriverConfig.load(URL, props("topic." + longest + ".mode", "append")).topic(longest).mode());
            fails(URL, props("topic." + longest + "t.mode", "append"));
        }
    }
}
