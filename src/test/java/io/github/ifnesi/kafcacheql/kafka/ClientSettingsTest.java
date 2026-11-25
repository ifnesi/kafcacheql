package io.github.ifnesi.kafcacheql.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ClientSettingsTest {

    private static final String SASL_PASSWORD = "S3CR3T-sasl-password";
    private static final String SR_USER_INFO = "SRKEY:S3CR3T-registry-secret";

    /** The shape of the client.properties Confluent Cloud generates for Java clients. */
    private static final String CONFLUENT_CLOUD = """
            bootstrap.servers=pkc-abcde.eu-west-1.aws.confluent.cloud:9092
            security.protocol=SASL_SSL
            sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='KEY' password='%s';
            sasl.mechanism=PLAIN
            client.dns.lookup=use_all_dns_ips
            session.timeout.ms=45000
            acks=all
            schema.registry.url=https://psrc-abcde.eu-west-1.aws.confluent.cloud
            basic.auth.credentials.source=USER_INFO
            basic.auth.user.info=%s
            """.formatted(SASL_PASSWORD, SR_USER_INFO);

    @TempDir
    Path dir;

    private DriverConfig cloud() throws Exception {
        Path f = dir.resolve("client.properties");
        Files.writeString(f, CONFLUENT_CLOUD);
        return DriverConfig.load("jdbc:kafcacheql://?config.file=" + f, null);
    }

    private static Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    private static ClientSettings of(String... kv) throws SQLException {
        return ClientSettings.of(DriverConfig.load("jdbc:kafcacheql://localhost:9092", props(kv)));
    }

    @Test
    void confluentCloudClientPropertiesReachEachClient() throws Exception {
        ClientSettings s = ClientSettings.of(cloud());

        Map<String, Object> c = s.consumer();
        assertEquals("pkc-abcde.eu-west-1.aws.confluent.cloud:9092", c.get("bootstrap.servers"));
        assertEquals("SASL_SSL", c.get("security.protocol"));
        assertEquals("PLAIN", c.get("sasl.mechanism"));
        assertTrue(c.get("sasl.jaas.config").toString().contains(SASL_PASSWORD), "unchanged, unshaded");
        assertEquals("45000", c.get("session.timeout.ms"));
        assertFalse(c.containsKey("acks"), "a producer setting");
        assertFalse(c.containsKey("schema.registry.url"));

        Map<String, Object> a = s.admin();
        assertEquals("SASL_SSL", a.get("security.protocol"));
        assertFalse(a.containsKey("session.timeout.ms"), "a consumer setting");
        assertFalse(a.containsKey("basic.auth.user.info"));

        Map<String, Object> r = s.schemaRegistry();
        assertEquals("https://psrc-abcde.eu-west-1.aws.confluent.cloud", r.get("schema.registry.url"));
        assertEquals(SR_USER_INFO, r.get("basic.auth.user.info"));
    }

    @Test
    void theFixedConsumerSettingsAreAlwaysThere() throws SQLException {
        Map<String, Object> c = of().consumer();
        assertNull(c.get("group.id"));
        assertEquals("false", c.get("enable.auto.commit"));
        assertEquals("none", c.get("auto.offset.reset"));
        assertEquals("read_committed", c.get("isolation.level"));
        assertEquals("false", c.get("allow.auto.create.topics"));
        assertEquals(ByteArrayDeserializer.class, c.get("key.deserializer"));
        assertEquals(ByteArrayDeserializer.class, c.get("value.deserializer"));
    }

    @ParameterizedTest
    @CsvSource({
            "group.id,my-group", "enable.auto.commit,true", "auto.offset.reset,earliest",
            "isolation.level,read_uncommitted", "allow.auto.create.topics,true",
            "key.deserializer,org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer,io.confluent.kafka.serializers.KafkaAvroDeserializer"})
    void overridingAFixedSettingFails(String key, String value) {
        SQLException e = assertThrows(SQLException.class, () -> of(key, value));
        assertTrue(e.getMessage().startsWith(key), e.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
            "enable.auto.commit,false", "auto.offset.reset,none", "isolation.level,read_committed",
            "allow.auto.create.topics,false",
            "value.deserializer,org.apache.kafka.common.serialization.ByteArrayDeserializer"})
    void aFixedSettingWithItsFixedValueIsAccepted(String key, String value) throws SQLException {
        of(key, value);
    }

    @Test
    void invalidClientSettingsFailBeforeAnyClientExists() {
        SQLException e = assertThrows(SQLException.class, () -> of("session.timeout.ms", "forty-five"));
        assertTrue(e.getMessage().contains("session.timeout.ms"), e.getMessage());
        assertThrows(SQLException.class, () -> of("security.protocol", "TELEPATHY"));
        assertThrows(SQLException.class, () -> of("schema.registry.url", "http://r:8081",
                "value.subject.name.strategy", "com.example.NoSuchStrategy"));
    }

    @Test
    void offlineSkipsValidationSinceNoClientIsCreated() throws SQLException {
        ClientSettings.of(DriverConfig.load("jdbc:kafcacheql://?offline=true&cache.path=/tmp/x.db", null));
    }

    @Test
    void unknownSettingsAreWarnedAboutByNameOnly() throws SQLException {
        LogCapture.start();
        of("append.default.windw", "2h", "my.vault.password", "S3CR3T-typo",
                // Schema Registry client settings that no Confluent definition lists are still known.
                "use.apache.http.client", "true", "max.retries", "1", "request.header.X-Trace", "t");
        String logs = String.join("\n", LogCapture.stop());
        assertFalse(logs.contains("use.apache.http.client") || logs.contains("max.retries")
                || logs.contains("request.header."), logs);
        assertTrue(logs.contains("'append.default.windw'"), logs);
        assertTrue(logs.contains("'my.vault.password'"), logs);
        assertFalse(logs.contains("S3CR3T"), logs);
        assertFalse(logs.contains("2h"), logs);
    }

}
