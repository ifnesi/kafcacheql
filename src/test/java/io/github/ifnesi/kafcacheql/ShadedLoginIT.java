package io.github.ifnesi.kafcacheql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Through the shaded JAR, a client.properties shaped like Confluent Cloud's (sasl.jaas.config naming
 * org.apache.kafka...PlainLoginModule) logs in to the stack's SASL/PLAIN listener on 9094.
 * Without the rewrite to relocated class names it can't.
 */
class ShadedLoginIT {

    private static final String SASL_BOOTSTRAP = System.getProperty("test.kafka.sasl.bootstrap", "localhost:9094");

    @TempDir
    static Path dir;

    @BeforeAll
    static void stack() {
        TestStack.requireUp();
    }

    @Test
    void confluentCloudStyleSettingsLogInThroughTheShadedJar() throws Exception {
        Map<String, String> out = probe(config("test-secret"), "rewritten");
        assertEquals("io.github.ifnesi.kafcacheql.shaded.org.apache.kafka.common.security.plain.PlainLoginModule",
                out.get("module"));
        assertEquals("ok", out.get("login"), out.toString());
        assertEquals(TestStack.admin().describeCluster().clusterId().get(), out.get("clusterId"));
    }

    /** JAAS allows comments and quoted option keys; neither may break the rewrite for the shaded JAR. */
    @Test
    void commentsInTheJaasConfigDontBreakTheLogin() throws Exception {
        for (String jaas : java.util.List.of(
                "/* a comment */ org.apache.kafka.common.security.plain.PlainLoginModule required username='test' password='test-secret';",
                "// a line comment\\norg.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test-secret\";",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='test' password='test-secret' \"ignored option\"='x';")) {
            Map<String, String> out = probe(config(jaas), "rewritten");
            assertEquals("ok", out.get("login"), jaas + " -> " + out);
        }
    }

    /** Through the driver, from a thread whose context class loader can't see the relocated login module. */
    @Test
    void theDriverLogsInWhateverTheCallersContextClassLoader() throws Exception {
        Map<String, String> out = probe(config("test-secret"), "driver-blind");
        assertEquals("ok", out.get("login"), out.toString());
        assertEquals(TestStack.admin().describeCluster().clusterId().get(), out.get("clusterId"));
    }

    @Test
    void withoutTheRewriteTheLoginModuleIsntFound() throws Exception {
        Map<String, String> out = probe(config("test-secret"), "raw");
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule", out.get("module"));
        assertEquals("failed", out.get("login"));
        assertTrue(out.get("error").contains("LoginModule"), out.get("error"));
    }

    @Test
    void theListenerChecksThePassword() throws Exception {
        Map<String, String> out = probe(config("wrong-password"), "rewritten");
        assertEquals("failed", out.get("login"));
        assertTrue(out.get("error").contains("Authentication failed"), out.get("error"));
        assertFalse(out.get("error").contains("wrong-password"), "no credential in the error");
    }

    private static Path config(String passwordOrJaas) throws Exception {
        String jaas = passwordOrJaas.contains("LoginModule") ? passwordOrJaas
                : "org.apache.kafka.common.security.plain.PlainLoginModule required username='test' password='"
                        + passwordOrJaas + "';";
        Path f = Files.createTempFile(dir, "client", ".properties");
        Files.writeString(f, """
                bootstrap.servers=%s
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=%s
                """.formatted(SASL_BOOTSTRAP, jaas));
        return f;
    }

    private static Map<String, String> probe(Path configFile, String mode) throws Exception {
        Path source = Path.of(ShadedLoginIT.class.getResource("/probe/ShadedLoginProbe.java").toURI());
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process p = new ProcessBuilder(java, "-cp", System.getProperty("shadedJar"), source.toString(),
                "jdbc:kafcacheql://?config.file=" + configFile, mode, "jdbc:kafcacheql://" + TestStack.BOOTSTRAP)
                .redirectErrorStream(true).start();
        assertTrue(p.waitFor(90, TimeUnit.SECONDS), "probe finished");
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.exitValue(), output);
        Map<String, String> values = new HashMap<>();
        output.lines().forEach(line -> {
            int eq = line.indexOf('=');
            if (eq > 0 && !line.startsWith("SLF4J")) {
                values.put(line.substring(0, eq), line.substring(eq + 1));
            }
        });
        assertTrue(values.containsKey("login"), output);
        return values;
    }
}
