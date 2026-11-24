package io.github.ifnesi.kafcacheql.config;

/**
 * The boundary around work that makes Kafka or Confluent load classes by name: their config definitions
 * (built once, in static initialisers), settings validation, login modules and clients. They load those
 * classes through the thread's context class loader. Inside the boundary that is the driver's own, whatever
 * the caller's thread had; the caller's is put back after. A tool that loads the driver in a class loader of
 * its own keeps its own on the thread, which can't see the driver's relocated classes, and a definition that
 * fails to build in a static initialiser leaves its class unusable until the JVM ends.
 */
public final class DriverLoader {

    public static final ClassLoader LOADER = DriverLoader.class.getClassLoader();

    @FunctionalInterface
    public interface Work<T, E extends Exception> {
        T run() throws E;
    }

    private DriverLoader() {}

    public static <T, E extends Exception> T call(Work<T, E> work) throws E {
        Thread thread = Thread.currentThread();
        ClassLoader caller = thread.getContextClassLoader();
        if (caller == LOADER) {
            return work.run();
        }
        thread.setContextClassLoader(LOADER);
        try {
            return work.run();
        } finally {
            thread.setContextClassLoader(caller);
        }
    }
}
