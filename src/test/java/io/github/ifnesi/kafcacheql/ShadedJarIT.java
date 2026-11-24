package io.github.ifnesi.kafcacheql;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** The shaded JAR. Runs after `package`; needs no Docker stack. */
class ShadedJarIT {

    private static final String PREFIX = "io/github/ifnesi/kafcacheql/shaded/";

    /** Libraries relocated under PREFIX. */
    private static final List<String> RELOCATED = List.of(
            "org/apache/kafka/", "io/confluent/", "org/apache/avro/", "com/fasterxml/jackson/",
            "net/sf/jsqlparser/", "com/google/", "org/apache/commons/", "org/apache/hc/",
            "org/yaml/snakeyaml/", "io/swagger/", "com/eclipsesource/json/",
            "org/checkerframework/", "javax/annotation/");

    /**
     * Kept in place: the driver's own classes (the prefix is inside them), JNI libraries (native symbols name
     * their packages) and the SLF4J API.
     */
    private static final List<String> KEPT = List.of(
            "io/github/ifnesi/kafcacheql/", "org/sqlite/", "com/github/luben/zstd/", "org/xerial/snappy/",
            "net/jpountz/", "org/slf4j/");

    private static Path jar;

    @BeforeAll
    static void findJar() {
        String path = System.getProperty("shadedJar");
        assertNotNull(path, "the shadedJar system property is set by failsafe");
        jar = Path.of(path);
        assertTrue(Files.isRegularFile(jar), "shaded JAR exists: " + jar);
    }

    @Test
    void everyClassIsRelocatedOrKeptOnPurpose() throws IOException {
        List<String> strays = new ArrayList<>();
        try (JarFile file = new JarFile(jar.toFile())) {
            for (JarEntry entry : file.stream().toList()) {
                // Multi-release copies count as the package they belong to.
                String name = entry.getName().replaceFirst("^META-INF/versions/\\d+/", "");
                if (!name.endsWith(".class") || name.endsWith("module-info.class")) {
                    continue;
                }
                boolean ok = name.startsWith(PREFIX) || KEPT.stream().anyMatch(name::startsWith);
                if (!ok) {
                    strays.add(name);
                }
            }
        }
        assertEquals(List.of(), strays.stream().limit(20).toList(), "classes outside the prefix");
    }

    @Test
    void eachRelocatedLibraryIsPresentUnderThePrefix() throws IOException {
        try (JarFile file = new JarFile(jar.toFile())) {
            List<String> names = file.stream().map(JarEntry::getName).toList();
            List<Executable> checks = new ArrayList<>();
            for (String pkg : RELOCATED) {
                checks.add(() -> assertTrue(
                        names.stream().anyMatch(n -> n.startsWith(PREFIX + pkg) && n.endsWith(".class")),
                        "relocated " + pkg));
            }
            assertAll(checks);
        }
    }

    @Test
    void serviceFilesNameRelocatedClasses() throws IOException {
        List<String> strays = new ArrayList<>();
        try (JarFile file = new JarFile(jar.toFile())) {
            for (JarEntry entry : file.stream().toList()) {
                String name = entry.getName();
                if (!name.startsWith("META-INF/services/") || entry.isDirectory()) {
                    continue;
                }
                List<String> lines = new ArrayList<>(List.of(name.substring("META-INF/services/".length())));
                try (InputStream in = file.getInputStream(entry)) {
                    lines.addAll(new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList());
                }
                for (String line : lines) {
                    String cls = line.replaceAll("#.*", "").trim().replace('.', '/');
                    if (RELOCATED.stream().anyMatch(cls::startsWith)) {
                        strays.add(name + ": " + line);
                    }
                }
            }
        }
        assertEquals(List.of(), strays, "service files naming unrelocated classes");
    }

    @Test
    void onlyKafCacheQLIsAdvertisedAsADriver() throws IOException {
        try (JarFile file = new JarFile(jar.toFile())) {
            JarEntry entry = file.getJarEntry("META-INF/services/java.sql.Driver");
            assertNotNull(entry, "META-INF/services/java.sql.Driver");
            try (InputStream in = file.getInputStream(entry)) {
                List<String> drivers = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines()
                        .map(l -> l.replaceAll("#.*", "").trim()).filter(l -> !l.isEmpty()).toList();
                assertEquals(List.of("io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver"), drivers);
            }
        }
    }

    /** Loads the JAR on its own, without the test classpath, and runs each native library. */
    @Test
    void relocatedAndNativeLibrariesWorkFromTheJarAlone() throws Exception {
        URL[] urls = {jar.toUri().toURL()};
        try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            String shaded = PREFIX.replace('/', '.');
            Class.forName(shaded + "org.apache.kafka.clients.consumer.KafkaConsumer", true, loader);
            Class.forName(shaded + "io.confluent.kafka.serializers.KafkaAvroDeserializer", true, loader);

            Driver sqlite = (Driver) Class.forName("org.sqlite.JDBC", true, loader)
                    .getDeclaredConstructor().newInstance();
            try (Connection c = sqlite.connect("jdbc:sqlite::memory:", new Properties());
                    Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("SELECT sqlite_version(), json_valid('{}')")) {
                assertTrue(rs.next());
                assertFalse(rs.getString(1).isBlank());
                assertEquals(1, rs.getInt(2));
            }

            byte[] data = "kafcacheql ".repeat(100).getBytes(StandardCharsets.UTF_8);
            Object zstd = Class.forName("com.github.luben.zstd.Zstd", true, loader)
                    .getMethod("compress", byte[].class).invoke(null, (Object) data);
            Object snappy = Class.forName("org.xerial.snappy.Snappy", true, loader)
                    .getMethod("compress", byte[].class).invoke(null, (Object) data);
            Object lz4 = Class.forName("net.jpountz.lz4.LZ4Factory", true, loader)
                    .getMethod("nativeInstance").invoke(null);
            assertTrue(((byte[]) zstd).length < data.length, "zstd native");
            assertTrue(((byte[]) snappy).length < data.length, "snappy native");
            assertNotNull(lz4, "lz4 native");
        }
    }
}
