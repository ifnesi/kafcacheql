package io.github.ifnesi.kafcacheql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The shaded JAR, beside a different kafka-clients, is found by DriverManager. Runs a probe
 * in a JVM of its own: DriverManager only hands out drivers the caller's class loader can see, so an
 * isolated class loader inside the test JVM can't show it. Needs no Docker stack.
 */
class DriverManagerIT {

    @Test
    void driverManagerFindsTheShadedDriverBesideAnotherKafkaClients() throws Exception {
        Path shaded = Path.of(System.getProperty("shadedJar"));
        Path otherKafka = Path.of(System.getProperty("otherKafkaClients"));
        assertTrue(Files.isRegularFile(shaded), shaded.toString());
        assertTrue(Files.isRegularFile(otherKafka), otherKafka.toString());

        Path cache = Files.createTempDirectory("driver-manager-it").resolve("cache.db");
        Map<String, String> out = probe(shaded + File.pathSeparator + otherKafka, "jdbc:kafcacheql://localhost:9092", cache);
        assertEquals("io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver", out.get("driver"));
        assertEquals("null", out.get("foreign"), "connect() returns null for other URLs");
        assertEquals("kafka-clients-3.9.1.jar", out.get("kafka"), "the application's own kafka-clients");
        assertEquals(shaded.getFileName().toString(), out.get("shaded"), "the driver's relocated copy");
        assertEquals("not registered", out.get("sqlite"), "only KafCacheQL registers");
        assertEquals("[io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver]", out.get("driversAfterCache"),
                "still only KafCacheQL once a cache is open");
    }

    private static Map<String, String> probe(String classPath, String url, Path cache) throws Exception {
        Path source = probeSource();
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process p = new ProcessBuilder(java, "-cp", classPath, source.toString(), url, cache.toString())
                .redirectErrorStream(true).start();
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "probe finished");
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.exitValue(), output);
        Map<String, String> values = new HashMap<>();
        for (String line : output.lines().toList()) {
            int eq = line.indexOf('=');
            if (eq > 0) {
                values.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }
        assertTrue(values.keySet().containsAll(List.of("driver", "foreign", "kafka", "shaded", "sqlite", "driversAfterCache")),
                output);
        return values;
    }

    private static Path probeSource() throws URISyntaxException {
        return Path.of(DriverManagerIT.class.getResource("/probe/DriverManagerProbe.java").toURI());
    }
}
