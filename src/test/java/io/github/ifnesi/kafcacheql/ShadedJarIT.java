package io.github.ifnesi.kafcacheql;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.Set;
import java.util.stream.Collectors;
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
            "org/jline/", "org/apache/kafka/", "io/confluent/", "org/apache/avro/", "com/fasterxml/jackson/",
            "net/sf/jsqlparser/", "com/google/", "org/apache/commons/", "org/apache/hc/",
            "com/squareup/", "com/palantir/javapoet/", "okio/",
            "org/yaml/snakeyaml/", "io/swagger/", "com/eclipsesource/json/",
            "org/checkerframework/", "javax/annotation/",
            "org/everit/", "org/json/", "com/damnhandy/", "com/github/erosb/", "com/sanctionco/", "org/joda/", "com/kjetland/", "javax/validation/", "io/github/classgraph/", "nonapi/io/github/classgraph/", "kotlin/", "org/intellij/", "org/jetbrains/");

    /**
     * Kept in place: the driver's own classes (the prefix is inside them), JNI libraries (native symbols name
     * their packages) and the SLF4J API.
     */
    private static final List<String> KEPT = List.of(
            "io/github/ifnesi/kafcacheql/", "org/sqlite/", "com/github/luben/zstd/", "org/xerial/snappy/",
            "net/jpountz/", "org/slf4j/", "org/jline/nativ/");

    private static Path jar;

    @BeforeAll
    static void findJar() {
        String path = System.getProperty("shadedJar");
        assertNotNull(path, "the shadedJar system property is set by failsafe");
        jar = Path.of(path);
        assertTrue(Files.isRegularFile(jar), "shaded JAR exists: " + jar);
    }

    /** PLAN §7.4: the public hook stays unrelocated and can be built against just the shaded driver. */
    @Test void publicTransformCompilesAndRunsAgainstTheShadedJar(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Path source = dir.resolve("UserTransform.java");
        Files.writeString(source, """
                import io.github.ifnesi.kafcacheql.transform.RecordTransform;
                public class UserTransform implements RecordTransform {
                    public Record apply(Record record) {
                        record.value(java.util.Map.of("safe", true));
                        record.headers(java.util.List.of(new Header("safe", new byte[]{1})));
                        return record;
                    }
                }
                """);
        assertEquals(0, javax.tools.ToolProvider.getSystemJavaCompiler().run(null, System.out, System.err,
                "--release", "21", "-cp", jar.toString(), "-d", dir.toString(), source.toString()));
        try (var loader = new URLClassLoader(new URL[]{jar.toUri().toURL(), dir.toUri().toURL()}, ClassLoader.getPlatformClassLoader())) {
            var api = loader.loadClass("io.github.ifnesi.kafcacheql.transform.RecordTransform");
            var record = loader.loadClass(api.getName() + "$Record");
            Object input = record.getConstructors()[0].newInstance("orders", 0, 1L, 2L, null, null, "key", null, List.of());
            Object hook = loader.loadClass("UserTransform").getConstructor().newInstance();
            Object output = api.getMethod("apply", record).invoke(hook, input);
            assertEquals(java.util.Map.of("safe", true), record.getMethod("value").invoke(output));
            assertEquals("key", record.getMethod("key").invoke(output));
            assertEquals("1", api.getMethod("version").invoke(hook));
        }
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
                boolean jlineProvider = name.startsWith("META-INF/jline/providers/");
                if ((!name.startsWith("META-INF/services/") && !jlineProvider) || entry.isDirectory()) {
                    continue;
                }
                List<String> lines = new ArrayList<>();
                if (!jlineProvider) lines.add(name.substring("META-INF/services/".length()));
                try (InputStream in = file.getInputStream(entry)) {
                    lines.addAll(new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList());
                }
                for (String line : lines) {
                    String cls = line.replaceAll("#.*", "").trim().replace('.', '/');
                    if (jlineProvider && !cls.isEmpty()) assertNotNull(file.getJarEntry(cls + ".class"), name + ": " + line);
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
            assertNull(file.getJarEntry("META-INF/services/org.slf4j.spi.SLF4JServiceProvider"),
                    "the CLI provider is explicit; libraries keep the host provider");
            JarEntry entry = file.getJarEntry("META-INF/services/java.sql.Driver");
            assertNotNull(entry, "META-INF/services/java.sql.Driver");
            try (InputStream in = file.getInputStream(entry)) {
                List<String> drivers = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines()
                        .map(l -> l.replaceAll("#.*", "").trim()).filter(l -> !l.isEmpty()).toList();
                assertEquals(List.of("io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver"), drivers);
            }
        }
    }

    /** 0001c / PLAN §14: only Linux, macOS and Windows, x86-64 and ARM64 natives.
     * Keep available musl builds too. This lz4-java version has no Windows ARM64 native.
     */
    @Test
    void nativePlatformsAreExactlyTheSupportedSubset() throws IOException {
        Set<String> expected = Set.of(
                "org/jline/nativ/Linux/x86_64", "org/jline/nativ/Linux/arm64",
                "org/jline/nativ/Linux-musl/x86_64", "org/jline/nativ/Linux-musl/arm64",
                "org/jline/nativ/Mac/x86_64", "org/jline/nativ/Mac/arm64",
                "org/jline/nativ/Windows/x86_64", "org/jline/nativ/Windows/arm64",
                "org/sqlite/native/Linux/x86_64", "org/sqlite/native/Linux/aarch64",
                "org/sqlite/native/Linux-Musl/x86_64", "org/sqlite/native/Linux-Musl/aarch64",
                "org/sqlite/native/Mac/x86_64", "org/sqlite/native/Mac/aarch64",
                "org/sqlite/native/Windows/x86_64", "org/sqlite/native/Windows/aarch64",
                "linux/amd64", "linux/aarch64", "darwin/x86_64", "darwin/aarch64", "win/amd64", "win/aarch64",
                "org/xerial/snappy/native/Linux/x86_64", "org/xerial/snappy/native/Linux/aarch64",
                "org/xerial/snappy/native/Linux/x86_64-musl",
                "org/xerial/snappy/native/Mac/x86_64", "org/xerial/snappy/native/Mac/aarch64",
                "org/xerial/snappy/native/Windows/x86_64", "org/xerial/snappy/native/Windows/aarch64",
                "net/jpountz/util/linux/amd64", "net/jpountz/util/linux/aarch64",
                "net/jpountz/util/darwin/x86_64", "net/jpountz/util/darwin/aarch64", "net/jpountz/util/windows/amd64");
        try (JarFile file = new JarFile(jar.toFile())) {
            Set<String> platforms = file.stream().map(JarEntry::getName)
                    .filter(n -> n.endsWith(".so") || n.endsWith(".dll") || n.endsWith(".dylib")
                            || n.endsWith(".jnilib") || n.endsWith(".a"))
                    .map(n -> n.substring(0, n.lastIndexOf('/'))).collect(Collectors.toSet());
            assertEquals(expected, platforms);
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
            Class.forName(shaded + "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer", true, loader);
            Class.forName(shaded + "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer", true, loader);
            var protobuf = Class.forName(shaded + "io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema", true, loader);
            Object proto = protobuf.getConstructor(String.class).newInstance("syntax = \"proto3\"; message Row { int64 n = 1; }");
            Object descriptor = protobuf.getMethod("toDescriptor").invoke(proto);
            assertNotNull(descriptor);
            var descriptorType = Class.forName(shaded + "com.google.protobuf.Descriptors$Descriptor", true, loader);
            var dynamic = Class.forName(shaded + "com.google.protobuf.DynamicMessage", true, loader);
            Object message = dynamic.getMethod("getDefaultInstance", descriptorType).invoke(null, descriptor);
            Object printer = Class.forName(shaded + "com.google.protobuf.util.JsonFormat", true, loader).getMethod("printer").invoke(null);
            printer = printer.getClass().getMethod("omittingInsignificantWhitespace").invoke(printer);
            assertEquals("{}", printer.getClass().getMethod("print",
                    Class.forName(shaded + "com.google.protobuf.MessageOrBuilder", true, loader)).invoke(printer, message));
            var jsonSchema = Class.forName(shaded + "io.confluent.kafka.schemaregistry.json.JsonSchema", true, loader);
            // Exercise both bundled validators, including their resources, from the standalone JAR.
            for (String dialect : List.of("http://json-schema.org/draft-07/schema#", "https://json-schema.org/draft/2020-12/schema")) {
                Object schema = jsonSchema.getConstructor(String.class).newInstance("{\"$schema\":\"" + dialect
                        + "\",\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}}}");
                assertNotNull(jsonSchema.getMethod("rawSchema").invoke(schema));
            }

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
