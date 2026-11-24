package io.github.ifnesi.kafcacheql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The driver as tools load it: the shaded JAR in a class loader of its own, called from a thread whose context
 * class loader can't see it (here the platform class loader, which sees no driver or Kafka class at all). Each
 * test loads a fresh copy of the JAR, so what it does first is the driver's first use.
 */
class IsolatedLoaderIT {

    private static final String SASL_BOOTSTRAP = System.getProperty("test.kafka.sasl.bootstrap", "localhost:9094");
    private static final ClassLoader BLIND = ClassLoader.getPlatformClassLoader();

    @BeforeAll
    static void stack() {
        TestStack.requireUp();
    }

    /** Validation builds Kafka's config definitions and loads the classes settings name, bundled ones included. */
    @Test
    void firstUseValidatesSettingsWhateverTheCallersContextClassLoader() throws Exception {
        try (URLClassLoader jar = isolatedDriver()) {
            Driver driver = (Driver) jar.loadClass("io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver")
                    .getDeclaredConstructor().newInstance();
            Properties p = new Properties();
            p.setProperty("sasl.oauthbearer.jwt.retriever.class",
                    "org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever");
            withContext(BLIND, () -> {
                for (int i = 0; i < 2; i++) {
                    SQLException e = assertThrows(SQLException.class,
                            () -> driver.connect("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p));
                    assertTrue(e.getMessage().contains("not built yet"), e.toString());
                    assertSame(BLIND, Thread.currentThread().getContextClassLoader(), "the caller's is put back");
                }
            });
        }
    }

    @Test
    void firstUseLogsInWhateverTheCallersContextClassLoader() throws Exception {
        String expected = TestStack.admin().describeCluster().clusterId().get();
        try (URLClassLoader jar = isolatedDriver()) {
            Class<?> configClass = jar.loadClass("io.github.ifnesi.kafcacheql.config.DriverConfig");
            Method acquire = jar.loadClass("io.github.ifnesi.kafcacheql.kafka.KafkaClients")
                    .getMethod("acquire", configClass);
            Properties p = new Properties();
            p.setProperty("security.protocol", "SASL_PLAINTEXT");
            p.setProperty("sasl.mechanism", "PLAIN");
            p.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username='test' password='test-secret';");
            withContext(BLIND, () -> {
                Object config = invoke(configClass.getMethod("load", String.class, Properties.class), null,
                        "jdbc:kafcacheql://" + SASL_BOOTSTRAP, p);
                try (AutoCloseable lease = (AutoCloseable) invoke(acquire, null, config)) {
                    Object clients = invoke(lease.getClass().getMethod("clients"), lease);
                    assertEquals(expected, invoke(clients.getClass().getMethod("clusterId"), clients));
                }
                assertSame(BLIND, Thread.currentThread().getContextClassLoader(), "the caller's is put back");
            });
        }
    }

    private static URLClassLoader isolatedDriver() throws Exception {
        URL jar = Path.of(System.getProperty("shadedJar")).toUri().toURL();
        return new URLClassLoader(new URL[] {jar}, ClassLoader.getPlatformClassLoader());
    }

    private interface Body {
        void run() throws Exception;
    }

    private static void withContext(ClassLoader loader, Body body) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            body.run();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private static Object invoke(Method m, Object target, Object... args) throws Exception {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception x) {
                throw x;
            }
            throw new AssertionError("failed on first use", e.getCause());
        }
    }
}
