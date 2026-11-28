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
import java.util.Set;
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
    // PLAN §14: Confluent's versions may emit only the JDK's four-line Unsafe diagnostic.
    // Keep raw stderr (and print it below); all other output still faces the existing assertions.
    private static final java.util.regex.Pattern UNSAFE_WARNING = java.util.regex.Pattern.compile(
            "(?m)^WARNING: A terminally deprecated method in sun\\.misc\\.Unsafe has been called\\R"
            + "WARNING: sun\\.misc\\.Unsafe::(\\w+) has been called by ([\\w.$]+) \\(file:[^\\r\\n]+\\)\\R"
            + "WARNING: Please consider reporting this to the maintainers of class \\2\\R"
            + "WARNING: sun\\.misc\\.Unsafe::\\1 will be removed in a future release\\R");
    private record Output(int status, String out, String err) {
        String messages() { return UNSAFE_WARNING.matcher(err).replaceAll(""); }
    }
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
            var result = new Output(process.exitValue(), Files.readString(out), errors);
            io.github.ifnesi.kafcacheql.testkit.SqlErrors.assertReadable(result.messages());
            return result;
        } finally { process.destroyForcibly(); }
    }
    private static String java() { return System.getProperty("test.cli.java", Path.of(System.getProperty("java.home"), "bin", "java").toString()); }
    private static String jar() { return Path.of(System.getProperty("shadedJar")).toAbsolutePath().toString(); }

    /** §14.1: each argument is one statement; aligned header/NULL, JSON numbers and nested JSON. */
    @Test void argumentsAndTableAndJsonOutput() throws Exception {
        String topic = topic(), url = url(topic);
        var table = run("", url, "SELECT n, NULL AS \"absent\" FROM \"" + topic + "\"", "SELECT 12345 AS wider");
        assertEquals(0, table.status(), table.messages());
        assertEquals("n | absent\n--+-------\n7 | NULL  \nwider\n-----\n12345\n", table.out());
        assertEquals("", table.messages(), "§14.1: no log output");
        assertFalse(table.messages().contains("No SLF4J providers"));
        assertFalse(table.messages().contains("no providers"));
        var json = run("", "--mode", "json", url,
                "SELECT n, NULL AS \"absent\", _key, _value, _headers, 'plain' AS text FROM \"" + topic + "\"",
                "SELECT 2 AS second");
        assertEquals(0, json.status(), json.messages());
        assertEquals("", json.messages(), "§14.1: no log output");
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
            var literal = run("", "--mode", "json", url, sql);
            assertEquals(0, literal.status(), literal.messages()); assertEquals("", literal.messages());
            var object = JSON.readTree(literal.out()).get(0);
            for (String column : List.of("_value", "_key", "_headers")) {
                assertTrue(object.get(column).isTextual(), literal.out());
                assertEquals(text, object.get(column).textValue());
            }
        }
        var nested = run("", "--mode", "json", url,
                "SELECT ' 123 ' AS _value, 'true' AS _key, 'null' AS _headers UNION ALL SELECT '[1]' AS _value, '{}' AS _key, '\"text\"' AS _headers");
        assertEquals(0, nested.status(), nested.messages()); assertEquals("", nested.messages());
        var complete = JSON.readTree(nested.out());
        assertTrue(complete.get(0).get("_value").isNumber()); assertTrue(complete.get(0).get("_key").booleanValue());
        assertTrue(complete.get(0).get("_headers").isNull()); assertTrue(complete.get(1).get("_value").isArray());
        assertTrue(complete.get(1).get("_key").isObject()); assertEquals("text", complete.get(1).get("_headers").textValue());
        var failure = run("", url, "SELECT missing FROM \"" + topic + "\"", "SELECT 999 AS never");
        assertEquals(1, failure.status()); assertFalse(failure.out().contains("never"));
        assertTrue(failure.messages().contains("missing"), failure.messages());
        assertEquals(1, failure.messages().lines().count(), "only the driver error");
        assertTrue(failure.messages().startsWith("ERROR: "), failure.messages());
        var missingFile = run("", "jdbc:kafcacheql://localhost:1?config.file=/nope.properties", "SHOW TABLES");
        assertEquals("ERROR: config.file /nope.properties doesn't exist.\n", missingFile.messages());
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        String brokers = "localhost:" + port;
        var unreachable = run("", "jdbc:kafcacheql://" + brokers + "?default.api.timeout.ms=500&request.timeout.ms=100", "SHOW TABLES");
        assertEquals("ERROR: Can't reach Kafka at " + brokers + ": timed out after 0.5 s.\n", unreachable.messages());
        assertEquals(1, run("").status(), "usage error");
        // §14.1: safe diagnostics and cells, schema queries on the selected Java runtime, native SQLite UNION types.
        String schemaTopic = TestStack.uniqueTopic("cli-schema");
        TestStack.createTopic(schemaTopic, 1, Map.of());
        var schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"CliSchema","fields":[{"name":"ms","type":"long"}]}
                """);
        var record = new org.apache.avro.generic.GenericData.Record(schema); record.put("ms", 42L);
        TestStack.sendAvro(schemaTopic, "k", record, null, TestStack.SchemaIdIn.PREFIX);
        String schemaUrl = url(schemaTopic) + "&topic." + schemaTopic + ".value.format=auto&schema.registry.url=" + TestStack.REGISTRY;
        var backed = run("", "--mode", "json", schemaUrl, "SELECT ms FROM \"" + schemaTopic + "\" WHERE _offset=0");
        assertEquals(0, backed.status(), backed.messages()); assertEquals("", backed.messages());
        assertEquals(42, JSON.readTree(backed.out()).get(0).get("ms").longValue());
        System.out.println("Avro CLI stderr:\n" + backed.err());
        assertAll(
                () -> {
                    var typo = run("", "jdbc:kafcacheq://localhost:9092?password=PLANTED-CLI-SECRET", "SHOW TABLES");
                    assertEquals(1, typo.status());
                    assertEquals("ERROR: Not a KafCacheQL URL: it must start with jdbc:kafcacheql:\n", typo.messages());
                    assertFalse(typo.out().contains("PLANTED-CLI-SECRET"));
                },
                () -> {
                    String control = "before\u001b[2Jafter\r\n\t\u0000\u007f\u0085";
                    // Kafka payload and a SQL column label must both be safe in a terminal.
                    TestStack.sendString(schemaTopic, "control", control, null);
                    String label = "head\u001b[2J\r\u0085";
                    String query = "SELECT _value ->> '$' AS '" + label + "' FROM \"" + schemaTopic + "\" WHERE _offset=1";
                    var cells = run("", schemaUrl, query);
                    assertEquals(0, cells.status(), cells.messages()); assertEquals("", cells.messages());
                    String heading = "head\\x1b[2J\\r\\x85", value = "before\\x1b[2Jafter\\r\\n\\t\\x00\\x7f\\x85";
                    int width = Math.max(heading.length(), value.length());
                    assertEquals(heading + " ".repeat(width-heading.length()) + "\n" + "-".repeat(width) + "\n" + value + "\n", cells.out());
                    var escaped = run("", "--mode", "json", schemaUrl, query);
                    assertEquals(0, escaped.status(), escaped.messages()); assertEquals("", escaped.messages());
                    assertEquals(control, JSON.readTree(escaped.out()).get(0).get(label).textValue());
                    assertFalse(escaped.out().contains("\u001b")); assertFalse(escaped.out().contains("\r"));
                },
                () -> {
                    String direct = "SELECT ms FROM \"" + schemaTopic + "\" WHERE _offset=0", literal = "SELECT 1.5 AS ms";
                    for (String query : List.of(direct + " UNION ALL " + literal, literal + " UNION ALL " + direct)) {
                        var union = run("", "--mode", "json", schemaUrl, query);
                        assertEquals(0, union.status(), union.messages()); assertEquals("", union.messages());
                        var values = JSON.readTree(union.out());
                        assertEquals(query.startsWith(direct) ? 42 : 1.5, values.get(0).get("ms").doubleValue());
                        assertEquals(query.startsWith(direct) ? 1.5 : 42, values.get(1).get("ms").doubleValue());
                    }
                });
        // §14: each schema format works through the shaded JAR; only the JDK Unsafe warnings are allowed.
        String jsonTopic = TestStack.uniqueTopic("cli-json-schema");
        TestStack.createTopic(jsonTopic, 1, Map.of());
        var jsonSchema = new io.confluent.kafka.schemaregistry.json.JsonSchema(
                "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}");
        try (var serializer = new io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer<>()) {
            serializer.configure(Map.of("schema.registry.url", TestStack.REGISTRY), false);
            var envelope = io.confluent.kafka.schemaregistry.json.JsonSchemaUtils.envelope(jsonSchema, JSON.readTree("{\"id\":42}"));
            TestStack.send(jsonTopic, 0, null, null, serializer.serialize(jsonTopic, envelope), null);
        }
        String jsonUrl = url(jsonTopic) + "&topic." + jsonTopic + ".value.format=auto&schema.registry.url=" + TestStack.REGISTRY;
        var schemaJson = run("", "--mode", "json", jsonUrl, "SELECT id FROM \"" + jsonTopic + "\"");
        assertEquals(0, schemaJson.status(), schemaJson.messages()); assertEquals("", schemaJson.messages());
        assertEquals(42, JSON.readTree(schemaJson.out()).get(0).get("id").longValue());
        System.out.println("JSON Schema CLI stderr:\n" + schemaJson.err());
        String protobufTopic = TestStack.uniqueTopic("cli-protobuf");
        TestStack.createTopic(protobufTopic, 1, Map.of());
        var protobufSchema = new io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema(
                "syntax = \"proto3\"; message CliProto { int64 id = 1; }");
        var descriptor = protobufSchema.toDescriptor();
        var message = com.google.protobuf.DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 42L).build();
        try (var serializer = new io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer<com.google.protobuf.Message>()) {
            serializer.configure(Map.of("schema.registry.url", TestStack.REGISTRY), false);
            TestStack.send(protobufTopic, 0, null, null, serializer.serialize(protobufTopic, message), null);
        }
        String protobufUrl = url(protobufTopic) + "&topic." + protobufTopic + ".value.format=auto&schema.registry.url=" + TestStack.REGISTRY;
        var protobuf = run("", "--mode", "json", protobufUrl, "SELECT id FROM \"" + protobufTopic + "\"");
        assertEquals(0, protobuf.status(), protobuf.messages()); assertEquals("", protobuf.messages());
        assertEquals(42, JSON.readTree(protobuf.out()).get(0).get("id").longValue());
        System.out.println("Protobuf CLI stderr:\n" + protobuf.err());
    }

    /** §14.1 Output modes: line prints aligned names, NULL and escaped controls, with only a blank
     * line between rows; --mode selects table/line/json and rejects missing/unknown names and --json.
     */
    @Test void outputModesAndRejectedOptions() throws Exception {
        String topic = TestStack.uniqueTopic("cli-line");
        TestStack.createTopic(topic, 1, Map.of());
        for (String text : List.of("before\u001b[2Jafter\r\n\t\u0000\u007f\u0085", "João")) {
            TestStack.send(topic, 0, null, null, JSON.writeValueAsBytes(Map.of("text", text)), null);
        }
        String url = url(topic);
        String query = "SELECT _offset, _value ->> '$.text' AS \"漢字\", NULL AS café FROM \"" + topic + "\" ORDER BY _offset";
        String usage = "Usage: java -jar kafcacheql-jdbc.jar [--mode table|line|json] <jdbc-url> [statement ...]\n";
        String invalidMode = "ERROR: Mode must be table, line or json.\n" + usage;
        assertAll(
                () -> {
                    var line = run("", "--mode", "line", url, query);
                    assertEquals(0, line.status(), line.messages()); assertEquals("", line.messages());
                    assertEquals("_offset: 0\n   漢字: before\\x1b[2Jafter\\r\\n\\t\\x00\\x7f\\x85\n   café: NULL\n\n"
                            + "_offset: 1\n   漢字: João\n   café: NULL\n", line.out());
                    var empty = run("", "--mode", "line", url, query + " LIMIT 0");
                    assertEquals(0, empty.status(), empty.messages()); assertEquals("", empty.messages()); assertEquals("", empty.out());
                    var table = run("", "--mode", "table", url, query);
                    var defaults = run("", url, query);
                    assertEquals(0, table.status(), table.messages()); assertEquals("", table.messages());
                    assertEquals(0, defaults.status(), defaults.messages()); assertEquals("", defaults.messages());
                    assertEquals(defaults.out(), table.out());
                },
                () -> {
                    // §14.1: line rows stay separated across statements; empty results add nothing.
                    String first = "SELECT _offset AS n FROM \"" + topic + "\" WHERE _offset=0";
                    String second = "SELECT _offset AS n FROM \"" + topic + "\" WHERE _offset=1";
                    var statements = List.of(query + " LIMIT 0", first, second, query + " LIMIT 0", first, query + " LIMIT 0");
                    var arguments = new ArrayList<>(List.of("--mode", "line", url)); arguments.addAll(statements);
                    var consecutive = run("", arguments.toArray(String[]::new));
                    assertEquals(0, consecutive.status(), consecutive.messages()); assertEquals("", consecutive.messages());
                    assertEquals("n: 0\n\nn: 1\n\nn: 0\n", consecutive.out());
                    var script = run(String.join(";\n", statements) + ";\n", "--mode", "line", url);
                    assertEquals(0, script.status(), script.messages()); assertEquals("", script.messages());
                    assertEquals(consecutive.out(), script.out());
                },
                () -> {
                    // §14.1 Running it: --mode belongs before the URL, never among SQL arguments.
                    for (String[] args : List.of(new String[] {url, "--mode", "line", query},
                            new String[] {"--mode", "line", url, query, "--mode", "json"})) {
                        var late = run("", args);
                        assertEquals(1, late.status()); assertEquals("", late.out());
                        assertEquals("ERROR: Put --mode before the URL.\n" + usage, late.messages());
                    }
                },
                () -> {
                    var bogus = run("", "--mode", "bogus", url, query);
                    assertEquals(1, bogus.status()); assertEquals("", bogus.out()); assertEquals(invalidMode, bogus.messages());
                },
                () -> {
                    var missing = run("", "--mode");
                    assertEquals(1, missing.status()); assertEquals("", missing.out()); assertEquals(invalidMode, missing.messages());
                },
                () -> {
                    var removed = run("", "--json", url, query);
                    assertEquals(1, removed.status()); assertEquals("", removed.out());
                    assertEquals("ERROR: Unknown option: --json.\n" + usage, removed.messages());
                });
    }

    /** §14.1: stdin ends statements at a semicolon ending a line; first failure stops with status 1. */
    @Test void scriptStopsAtTheFirstFailure() throws Exception {
        String topic = topic(), url = url(topic);
        // §14.1: stdin or stdout redirected means script mode, even with a terminal present.
        pty(topic, url, "pipe");
        var result = run("SELECT\n 'a;b' AS text;\nSELECT n FROM \"" + topic + "\";\n"
                + "SELECT missing FROM \"" + topic + "\";\nSELECT 999 AS never;\n", "--mode", "json", url);
        assertEquals(1, result.status());
        assertEquals("[{\"text\":\"a;b\"}]\n[{\"n\":7}]\n", result.out());
        assertTrue(result.messages().contains("missing"), result.messages());
        assertEquals(0, run("SELECT n FROM \"" + topic + "\";\n", url).status());
        // §14.1 Help: scripts and arguments print neither greeting nor help; help is sent as SQL.
        var help = run("help;\nSELECT 999 AS never;\n", "--mode", "json", url);
        assertEquals(1, help.status()); assertEquals("", help.out());
        var argument = run("", "--mode", "json", url, "help;");
        assertEquals(1, argument.status()); assertEquals("", argument.out());
        assertTrue(help.messages().startsWith("ERROR: "), help.messages());
        assertEquals(argument.messages(), help.messages(), "the driver's SQL rejection, as for an argument");
    }

    /** §14.1: cache statements work as anywhere else; DROP prints its warning on stderr. */
    @Test void cacheStatementsAndWarnings() throws Exception {
        String topic = topic(), url = url(topic);
        var result = run("", "--mode", "json", url, "SYNC " + topic, "DROP TABLE \"" + topic + "\"");
        assertEquals(0, result.status(), result.messages());
        var row = JSON.readTree(result.out()).get(0);
        assertEquals(topic, row.get("table_name").textValue());
        assertEquals(1, row.get("fetched_records").longValue());
        assertTrue(result.messages().contains("Kafka is unchanged"), result.messages());
        assertFalse(result.messages().contains("No SLF4J providers"), result.messages());
        assertEquals(1, result.messages().lines().count(), "only the driver warning");
        assertTrue(result.messages().startsWith("WARN: "), result.messages());
        assertTrue(TestStack.topicExists(topic));
        // PLAN §8.7: SHOW patterns share SYNC syntax and matching; filters and offline visibility apply.
        String other = TestStack.uniqueTopic("cli-other"), filtered = TestStack.uniqueTopic("cli-filtered");
        TestStack.createTopic(other, 1, Map.of()); TestStack.createTopic(filtered, 1, Map.of());
        String listed = url + "&topics.include=" + topic + "|" + other + "|" + filtered + "&topics.exclude=" + filtered;
        try (var c = DriverManager.getConnection(listed); var statement = c.createStatement();
             var rows = statement.executeQuery("SELECT n FROM \"" + topic + "\"")) { assertTrue(rows.next()); }
        for (boolean offline : List.of(false, true)) {
            String target = listed + "&offline=" + offline;
            Set<String> all = offline ? Set.of(topic) : Set.of(topic, other);
            var cases = List.of(
                    Map.entry("*" + topic.substring(0, topic.length() - 1) + "?", Set.of(topic)),
                    Map.entry(topic + ", " + other, all),
                    Map.entry("\"" + topic + "\"", Set.of(topic)),
                    Map.entry("`" + topic + "`", Set.of(topic)),
                    Map.entry("[" + topic + "]", Set.of(topic)),
                    Map.entry(topic + ", *", all),
                    Map.entry(topic.toUpperCase(java.util.Locale.ROOT), Set.<String>of()),
                    Map.entry("no-such-" + topic, Set.<String>of()),
                    Map.entry(filtered, Set.<String>of()),
                    Map.entry(other, offline ? Set.<String>of() : Set.of(other)));
            var commands = new ArrayList<>(List.of("--mode", "json", target));
            cases.forEach(test -> commands.add("SHOW TABLES " + test.getKey()));
            var show = run("", commands.toArray(String[]::new));
            assertEquals(0, show.status(), show.messages()); assertEquals("", show.messages());
            var results = show.out().lines().toList(); assertEquals(cases.size(), results.size());
            for (int i = 0; i < cases.size(); i++) {
                var names = new java.util.HashSet<String>();
                var rows = JSON.readTree(results.get(i)); rows.forEach(table -> names.add(table.get("table_name").textValue()));
                assertEquals(cases.get(i).getValue(), names, cases.get(i).getKey());
                assertEquals(names.size(), rows.size(), "patterns must not duplicate rows");
            }
        }
    }

    /** §14.1: real terminal, prompt/quit/exit/EOF, errors continue, Ctrl-C cancels, in-memory history. */
    @Test void interactiveSessionThroughARealPty() throws Exception {
        String topic = topic(), url = url(topic) + "&topics.include=" + topic + ".*|shop-" + topic;
        TestStack.createTopic("shop-" + topic, 1, Map.of());
        TestStack.createTopic(topic + "-north-a", 1, Map.of());
        TestStack.createTopic(topic + "-north-b", 1, Map.of());
        TestStack.createTopic(topic + "-south", 1, Map.of());
        pty(topic, url);
    }

    private void pty(String topic, String url, String... mode) throws Exception {
        Path probe = Path.of(CliIT.class.getResource("/probe/CliPtyProbe.py").toURI());
        Path transcript = dir.resolve("pty.txt");
        String version;
        try (var connection = DriverManager.getConnection(url)) { version = connection.getMetaData().getDriverVersion(); }
        Path badInputs = dir.resolve("bad-inputs.json");
        JSON.writeValue(badInputs.toFile(), io.github.ifnesi.kafcacheql.testkit.SqlErrors.badInputs());
        Path refreshProbe = Path.of(CliIT.class.getResource("/probe/CliRefreshProbe.java").toURI());
        var command = new ArrayList<>(List.of("python3", probe.toString(), java(), System.getProperty("java.io.tmpdir"),
                jar(), url, topic, version, badInputs.toString(), TestStack.BOOTSTRAP,
                dir.resolve("cache.db").toRealPath().toString(), dir.toString(), refreshProbe.toString()));
        command.addAll(List.of(mode));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(transcript.toFile()).start();
        try {
            if (mode.length == 0) {
                // §14.1: a topic created after the prompt appears is picked up by the background refresh.
                TestStack.await(() -> Files.exists(dir.resolve("completion-ready")) || !process.isAlive(), "completion session starts");
                assertTrue(process.isAlive(), Files.readString(transcript));
                TestStack.createTopic(topic + "-later", 1, Map.of());
                Files.writeString(dir.resolve("completion-created"), "");
            }
            assertTrue(process.waitFor(100, TimeUnit.SECONDS), "PTY probe finishes");
            String output = Files.readString(transcript); System.out.print(output);
            assertEquals(0, process.exitValue(), output);
            io.github.ifnesi.kafcacheql.testkit.SqlErrors.assertReadable(output);
            assertTrue(output.contains("PTY checks passed"), output);
        } finally { process.destroyForcibly(); }
    }
}
