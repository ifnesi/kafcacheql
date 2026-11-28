package io.github.ifnesi.kafcacheql.kafka;

import static io.github.ifnesi.kafcacheql.testkit.SqlErrors.assertJdbcThrows;

import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** JDBC connections are not implemented yet: drive the real connect validation and cache sync entry points. */
class CredentialsIT {
    @TempDir Path dir;

    @BeforeAll static void stack() { TestStack.requireUp(); }

    private Properties properties() {
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("credentials.db").toString());
        p.setProperty("security.protocol", "SASL_PLAINTEXT");
        p.setProperty("sasl.mechanism", "PLAIN");
        p.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"test\" password=\"test-secret\";");
        p.setProperty("schema.registry.url", TestStack.REGISTRY.replace("://", "://REGISTRY-USER:REGISTRY-SECRET@"));
        p.setProperty("my.vault.password", "CUSTOM-SECRET");
        p.setProperty("request.header.Authorization", "Bearer HEADER-SECRET");
        return p;
    }

    private static String url() {
        return "jdbc:kafcacheql://" + System.getProperty("test.kafka.sasl.bootstrap", "localhost:9094");
    }

    /** PLAN §4.5: URL credentials move to hidden basic.auth.user.info; §4.7: "Never log them".
     * Check PASSWORD settings, credential-looking names and request.header.* on a real authenticated sync.
     */
    @Test void syncHidesCredentialsInLogsAndToString() throws Exception {
        String topic = TestStack.uniqueTopic("credentials");
        TestStack.createTopic(topic, 1, Map.of());
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"CredentialRecord","fields":[{"name":"id","type":"string"}]}
                """);
        var record = new GenericData.Record(schema);
        record.put("id", "one");
        TestStack.sendAvroKeyed(topic, "key", record, null, TestStack.SchemaIdIn.PREFIX);
        String rendered;
        String logs;
        LogCapture.start();
        try {
            DriverConfig config = DriverConfig.load(url(), properties());
            rendered = config + "\n" + ClientSettings.of(config);
            try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(20))) {
                long id = cache.sync(cache.plan(topic, execution), execution);
                assertEquals("{\"id\":\"one\"}", cache.read(c -> {
                    try (var s = c.createStatement(); var rs = s.executeQuery("SELECT _value FROM \"$t" + id + "\"")) {
                        assertTrue(rs.next());
                        String value = rs.getString(1);
                        assertFalse(rs.next());
                        return value;
                    }
                }));
            }
        } finally {
            logs = String.join("\n", LogCapture.stop());
        }
        assertTrue(logs.contains("AdminClientConfig values"), "admin logs captured");
        assertTrue(logs.contains("ConsumerConfig values"), "consumer logs captured");
        assertTrue(logs.contains("KafkaAvroDeserializerConfig values"), "decoder logs captured");
        for (String text : List.of(logs, rendered)) {
            assertHidden(text, "test-secret", "REGISTRY-USER", "REGISTRY-SECRET", "CUSTOM-SECRET", "HEADER-SECRET");
        }
    }

    /** PLAN §4.5: "Errors, and the causes they carry, are redacted: secret values, their fragments".
     * Real JDBC validation failures and a broker rejecting a login must reveal no credential.
     */
    @Test void connectionErrorsHideCredentials() throws Exception {
        org.junit.jupiter.api.Assertions.assertAll(
                () -> {
                    for (boolean offline : List.of(false, true)) for (String source : List.of("properties", "url", "file")) {
                        var p = new Properties(); p.setProperty("offline", Boolean.toString(offline));
                        p.setProperty("cache.path", System.getProperty("java.io.tmpdir") + "/refused-bootstrap.db");
                        String bootstrap = "alice:PLANTED-BOOTSTRAP-SECRET@localhost:9092", jdbc = "jdbc:kafcacheql://";
                        if (source.equals("url")) jdbc += "?bootstrap.servers=" + bootstrap;
                        else if (source.equals("properties")) p.setProperty("bootstrap.servers", bootstrap);
                        else {
                            var file = java.nio.file.Files.createTempFile(java.nio.file.Path.of(System.getProperty("java.io.tmpdir")), "bootstrap-", ".properties");
                            java.nio.file.Files.writeString(file, "bootstrap.servers=" + bootstrap);
                            p.setProperty("config.file", file.toString());
                        }
                        final String address = jdbc;
                        SQLException failure = assertJdbcThrows(SQLException.class, () -> new KafCacheQLDriver().connect(address, p));
                        assertTrue(failure.getMessage().contains("credentials")); assertHidden(trace(failure), "PLANTED-BOOTSTRAP-SECRET");
                    }
                },
                () -> {
                    for (String key : List.of("sasl.login.callback.handler.class", "sasl.client.callback.handler.class", "sasl.login.class")) {
                        Properties p = properties(); p.setProperty(key, "java.lang.String");
                        SQLException failure = assertJdbcThrows(SQLException.class, () -> new KafCacheQLDriver().connect(url(), p));
                        assertEquals(key + " must name a class that implements " + (key.equals("sasl.login.class") ? "Login." : "AuthenticateCallbackHandler."), failure.getMessage());
                    }
                });
        try (var capture = LogCapture.capture()) {
            Properties p = properties();
            p.setProperty("request.timeout.ms", "HEADER-SECRET");
            SQLException invalid = assertJdbcThrows(SQLException.class, () -> new KafCacheQLDriver().connect(url(), p));
            assertTrue(invalid.getMessage().contains("request.timeout.ms"));
            assertHidden(trace(invalid), "HEADER-SECRET");

            // Refuse unsafe URL forms before a client can log them. One generic diagnostic suffices.
            p.remove("request.timeout.ms");
            p.setProperty("schema.registry.url", "http://REGISTRY-USER:FIRST,SECOND@localhost:8081");
            DriverConfig malformed = DriverConfig.load(url(), p);
            assertHidden(malformed.toString(), "REGISTRY-USER", "FIRST", "SECOND");
            SQLException badUrl = assertJdbcThrows(SQLException.class, () -> new KafCacheQLDriver().connect(url(), p));
            assertTrue(badUrl.getMessage().contains("schema.registry.url"));
            assertHidden(trace(badUrl), "REGISTRY-USER", "FIRST", "SECOND");

            p.setProperty("schema.registry.url", TestStack.REGISTRY);
            p.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=test password=FIRST SECOND;");
            SQLException badJaas = assertJdbcThrows(SQLException.class, () -> new KafCacheQLDriver().connect(url(), p));
            assertTrue(badJaas.getMessage().contains("sasl.jaas.config can't be parsed"));
            assertHidden(trace(badJaas), "FIRST", "SECOND");

            p.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"test\" password=\"WRONG-SASL-SECRET\";");
            int before = KafkaClients.openCount();
            String logs;
            SQLException rejected;
            LogCapture.start();
            try {
                rejected = assertThrows(SQLException.class, () -> KafkaClients.acquire(DriverConfig.load(url(), p)));
            } finally {
                logs = String.join("\n", LogCapture.stop());
            }
            assertEquals("08006", rejected.getSQLState());
            assertTrue(trace(rejected).contains("SaslAuthenticationException"), trace(rejected));
            assertHidden(logs + trace(rejected), "WRONG-SASL-SECRET", "CUSTOM-SECRET", "HEADER-SECRET");
            assertEquals(before, KafkaClients.openCount(), "a failed open leaves no shared admin");
            String captured = String.join("\n", capture.lines());
            assertTrue(captured.contains("'my.vault.password' isn't a KafCacheQL property"));
            assertHidden(captured, "test-secret", "REGISTRY-USER", "REGISTRY-SECRET", "CUSTOM-SECRET", "HEADER-SECRET",
                    "FIRST", "SECOND", "WRONG-SASL-SECRET");
        }
    }

    private static String trace(Throwable e) {
        StringWriter text = new StringWriter();
        e.printStackTrace(new PrintWriter(text));
        return text.toString();
    }

    private static void assertHidden(String text, String... credentials) {
        for (String credential : credentials) {
            assertFalse(text.contains(credential), "credential leaked: " + credential);
        }
    }
}
