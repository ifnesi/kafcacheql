import io.github.ifnesi.jdbckafka.config.DriverConfig;
import io.github.ifnesi.jdbckafka.kafka.ClientSettings;
import io.github.ifnesi.jdbckafka.kafka.KafkaClients;
import io.github.ifnesi.jdbckafka.shaded.org.apache.kafka.clients.admin.Admin;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Run by ShadedLoginIT in a JVM of its own, compiled and run against the shaded JAR alone:
 * {@code java -cp <shaded JAR> ShadedLoginProbe.java <url> rewritten|raw|driver-blind [<plaintext url>]}.
 * Logs in with the admin client the driver's settings describe. "raw" puts back the user's sasl.jaas.config as
 * written, without the rewrite to relocated class names. "driver-blind" logs in through KafkaClients instead,
 * from a thread whose context class loader can't see the login module, after a first pair (the plaintext
 * url) opened from it. Prints key=value lines.
 */
public class ShadedLoginProbe {

    public static void main(String[] args) throws Exception {
        DriverConfig config = DriverConfig.load(args[0], null);
        Map<String, Object> admin = new HashMap<>(ClientSettings.of(config).admin());
        if (args[1].equals("raw")) {
            admin.put("sasl.jaas.config", config.clientSettings().get("sasl.jaas.config"));
        }
        System.out.println("module=" + admin.get("sasl.jaas.config").toString().split("\\s+")[0]);
        if (args[1].equals("driver-blind")) {
            throughTheDriverFromABlindContext(config, DriverConfig.load(args[2], null));
            return;
        }
        try (Admin client = Admin.create(admin)) {
            String id = client.describeCluster().clusterId().get(20, TimeUnit.SECONDS);
            System.out.println("login=ok");
            System.out.println("clusterId=" + id);
        } catch (Throwable t) {
            failed(t);
        }
    }

    private static void throughTheDriverFromABlindContext(DriverConfig sasl, DriverConfig plaintext) {
        Thread caller = Thread.currentThread();
        caller.setContextClassLoader(new ClassLoader(caller.getContextClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.endsWith(".security.plain.PlainLoginModule")) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        });
        try {
            try (KafkaClients.Lease warm = KafkaClients.acquire(plaintext)) {
                warm.clients();
            }
            try (KafkaClients.Lease lease = KafkaClients.acquire(sasl)) {
                System.out.println("login=ok");
                System.out.println("clusterId=" + lease.clients().clusterId());
            }
        } catch (Throwable t) {
            failed(t);
        }
    }

    private static void failed(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        System.out.println("login=failed");
        System.out.println("error=" + t.getMessage() + " / " + root.getClass().getSimpleName() + ": " + root.getMessage());
    }
}
