package io.github.ifnesi.kafcacheql;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** PLAN §14.1: the standalone shaded JAR is a JDBC client, exercised as a terminal/script user. */
class CliIT {
    @TempDir Path dir;
    private static final ObjectMapper JSON = new ObjectMapper();
    @BeforeAll static void stack() { TestStack.requireUp(); }

    private String topic() {
        String topic = TestStack.uniqueTopic("cli");
        TestStack.createTopic(topic, 1, Map.of("cleanup.policy", "delete"));
        TestStack.send(topic, 0, null, "{\"id\":42}".getBytes(StandardCharsets.UTF_8),
                "{\"n\":7,\"nested\":{\"ok\":true}}".getBytes(StandardCharsets.UTF_8),
                List.of(new RecordHeader("text", "hello".getBytes(StandardCharsets.UTF_8))));
        return topic;
    }
    private String url(String topic) throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("cache.db").toString());
        p.setProperty("topics.include", topic);
        p.setProperty("topic." + topic + ".key.format", "json");
        p.setProperty("topic." + topic + ".value.format", "json");
        Path config = dir.resolve("client.properties");
        try (var out = Files.newOutputStream(config)) { p.store(out, null); }
        return "jdbc:kafcacheql://" + TestStack.BOOTSTRAP + "?config.file=" + config;
    }
    private record Output(int status, String out, String err) {}
    private Output run(String input, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(java(), "-Xmx1g", "-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"), "-jar", jar()));
        command.addAll(List.of(args));
        Path out = Files.createTempFile(dir, "stdout", ".txt"), err = Files.createTempFile(dir, "stderr", ".txt");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        try {
            try (var stdin = process.getOutputStream()) { stdin.write(input.getBytes(StandardCharsets.UTF_8)); }
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "CLI finishes");
            String errors = Files.readString(err);
            System.err.print(errors); // Every warning remains visible.
            io.github.ifnesi.kafcacheql.testkit.SqlErrors.assertReadable(errors);
            return new Output(process.exitValue(), Files.readString(out), errors);
        } finally { process.destroyForcibly(); }
    }
    private static String java() { return System.getProperty("test.cli.java", Path.of(System.getProperty("java.home"), "bin", "java").toString()); }
    private static String jar() { return Path.of(System.getProperty("shadedJar")).toAbsolutePath().toString(); }

    /** §14.1: each argument is one statement; aligned header/NULL, JSON numbers and nested JSON. */
    @Test void argumentsAndTableAndJsonOutput() throws Exception {
        String topic = topic(), url = url(topic);
        var table = run("", url, "SELECT n, NULL AS \"absent\" FROM \"" + topic + "\"", "SELECT 12345 AS wider");
        assertEquals(0, table.status(), table.err());
        assertEquals("n | absent\n--+-------\n7 | NULL  \nwider\n-----\n12345\n", table.out());
        assertEquals("", table.err(), "§14.1: no log output");
        assertFalse(table.err().contains("No SLF4J providers"));
        assertFalse(table.err().contains("no providers"));
        var json = run("", "--json", url,
                "SELECT n, NULL AS \"absent\", _key, _value, _headers, 'plain' AS text FROM \"" + topic + "\"",
                "SELECT 2 AS second");
        assertEquals(0, json.status(), json.err());
        assertEquals("", json.err(), "§14.1: no log output");
        var lines = json.out().lines().toList();
        assertEquals(2, lines.size(), json.out());
        var row = JSON.readTree(lines.getFirst()).get(0);
        assertEquals(7, row.get("n").intValue()); assertTrue(row.get("n").isNumber());
        assertTrue(row.get("absent").isNull()); assertEquals("plain", row.get("text").textValue());
        assertEquals(42, row.get("_key").get("id").intValue());
        assertTrue(row.get("_value").get("nested").get("ok").booleanValue());
        assertEquals("hello", row.get("_headers").get(0).get("value").textValue());
        assertEquals(2, JSON.readTree(lines.get(1)).get(0).get("second").intValue());
        // §14.1: nesting applies only to exactly one complete JSON value; other text is unchanged.
        for (String text : List.of("123 trailing", "", "   ", "plain text", "123 456", "{} []")) {
            String sql = "SELECT '" + text + "' AS _value, '" + text + "' AS _key, '" + text + "' AS _headers";
            var literal = run("", "--json", url, sql);
            assertEquals(0, literal.status(), literal.err()); assertEquals("", literal.err());
            var object = JSON.readTree(literal.out()).get(0);
            for (String column : List.of("_value", "_key", "_headers")) {
                assertTrue(object.get(column).isTextual(), literal.out());
                assertEquals(text, object.get(column).textValue());
            }
        }
        var nested = run("", "--json", url,
                "SELECT ' 123 ' AS _value, 'true' AS _key, 'null' AS _headers UNION ALL SELECT '[1]' AS _value, '{}' AS _key, '\"text\"' AS _headers");
        assertEquals(0, nested.status(), nested.err()); assertEquals("", nested.err());
        var complete = JSON.readTree(nested.out());
        assertTrue(complete.get(0).get("_value").isNumber()); assertTrue(complete.get(0).get("_key").booleanValue());
        assertTrue(complete.get(0).get("_headers").isNull()); assertTrue(complete.get(1).get("_value").isArray());
        assertTrue(complete.get(1).get("_key").isObject()); assertEquals("text", complete.get(1).get("_headers").textValue());
        var failure = run("", url, "SELECT missing FROM \"" + topic + "\"", "SELECT 999 AS never");
        assertEquals(1, failure.status()); assertFalse(failure.out().contains("never"));
        assertTrue(failure.err().contains("missing"), failure.err());
        assertEquals(1, failure.err().lines().count(), "only the driver error");
        assertTrue(failure.err().startsWith("ERROR: "), failure.err());
        var missingFile = run("", "jdbc:kafcacheql://localhost:1?config.file=/nope.properties", "SHOW TABLES");
        assertEquals("ERROR: config.file /nope.properties doesn't exist.\n", missingFile.err());
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        String brokers = "localhost:" + port;
        var unreachable = run("", "jdbc:kafcacheql://" + brokers + "?default.api.timeout.ms=500&request.timeout.ms=100", "SHOW TABLES");
        assertEquals("ERROR: Can't reach Kafka at " + brokers + ": timed out after 0.5 s.\n", unreachable.err());
        assertEquals(1, run("").status(), "usage error");
    }

    /** §14.1: stdin ends statements at a semicolon ending a line; first failure stops with status 1. */
    @Test void scriptStopsAtTheFirstFailure() throws Exception {
        String topic = topic(), url = url(topic);
        // §14.1: stdin or stdout redirected means script mode, even with a terminal present.
        pty(topic, url, "pipe");
        var result = run("SELECT\n 'a;b' AS text;\nSELECT n FROM \"" + topic + "\";\n"
                + "SELECT missing FROM \"" + topic + "\";\nSELECT 999 AS never;\n", "--json", url);
        assertEquals(1, result.status());
        assertEquals("[{\"text\":\"a;b\"}]\n[{\"n\":7}]\n", result.out());
        assertTrue(result.err().contains("missing"), result.err());
        assertEquals(0, run("SELECT n FROM \"" + topic + "\";\n", url).status());
        // §14.1 Help: scripts and arguments print neither greeting nor help; help is sent as SQL.
        var help = run("help;\nSELECT 999 AS never;\n", "--json", url);
        assertEquals(1, help.status()); assertEquals("", help.out());
        var argument = run("", "--json", url, "help;");
        assertEquals(1, argument.status()); assertEquals("", argument.out());
        assertTrue(help.err().startsWith("ERROR: "), help.err());
        assertEquals(argument.err(), help.err(), "the driver's SQL rejection, as for an argument");
    }

    /** §14.1: cache statements work as anywhere else; DROP prints its warning on stderr. */
    @Test void cacheStatementsAndWarnings() throws Exception {
        String topic = topic(), url = url(topic);
        var result = run("", "--json", url, "SYNC " + topic, "DROP TABLE \"" + topic + "\"");
        assertEquals(0, result.status(), result.err());
        var row = JSON.readTree(result.out()).get(0);
        assertEquals(topic, row.get("table_name").textValue());
        assertEquals(1, row.get("fetched_records").longValue());
        assertTrue(result.err().contains("Kafka is unchanged"), result.err());
        assertFalse(result.err().contains("No SLF4J providers"), result.err());
        assertEquals(1, result.err().lines().count(), "only the driver warning");
        assertTrue(result.err().startsWith("WARN: "), result.err());
        assertTrue(TestStack.topicExists(topic));
    }

    /** §14.1: real terminal, prompt/quit/exit/EOF, errors continue, Ctrl-C cancels, in-memory history. */
    @Test void interactiveSessionThroughARealPty() throws Exception {
        String topic = topic(), url = url(topic);
        pty(topic, url);
    }

    private void pty(String topic, String url, String... mode) throws Exception {
        Path probe = Path.of(CliIT.class.getResource("/probe/CliPtyProbe.py").toURI());
        Path transcript = dir.resolve("pty.txt");
        String version;
        try (var connection = DriverManager.getConnection(url)) { version = connection.getMetaData().getDriverVersion(); }
        Path badInputs = dir.resolve("bad-inputs.json");
        JSON.writeValue(badInputs.toFile(), io.github.ifnesi.kafcacheql.testkit.SqlErrors.badInputs());
        var command = new ArrayList<>(List.of("python3", probe.toString(), java(), System.getProperty("java.io.tmpdir"), jar(), url, topic, version, badInputs.toString(), TestStack.BOOTSTRAP, dir.resolve("cache.db").toRealPath().toString()));
        command.addAll(List.of(mode));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(transcript.toFile()).start();
        try {
            assertTrue(process.waitFor(100, TimeUnit.SECONDS), "PTY probe finishes");
            String output = Files.readString(transcript); System.out.print(output);
            assertEquals(0, process.exitValue(), output);
            io.github.ifnesi.kafcacheql.testkit.SqlErrors.assertReadable(output);
            assertTrue(output.contains("PTY checks passed"), output);
        } finally { process.destroyForcibly(); }
    }
}
