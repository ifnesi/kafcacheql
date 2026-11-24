import io.github.ifnesi.jdbckafka.config.DriverConfig;
import io.github.ifnesi.jdbckafka.kafka.ClientSettings;
import io.github.ifnesi.jdbckafka.shaded.org.apache.kafka.clients.admin.Admin;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Run by ShadedLoginIT in a JVM of its own, compiled and run against the shaded JAR alone:
 * {@code java -cp <shaded JAR> ShadedLoginProbe.java <url> rewritten|raw}. Logs in with the admin client
 * the driver's settings describe. "raw" puts back the user's sasl.jaas.config as written, without the
 * rewrite to relocated class names. Prints key=value lines.
 */
public class ShadedLoginProbe {

    public static void main(String[] args) throws Exception {
        DriverConfig config = DriverConfig.load(args[0], null);
        Map<String, Object> admin = new HashMap<>(ClientSettings.of(config).admin());
        if (args[1].equals("raw")) {
            admin.put("sasl.jaas.config", config.clientSettings().get("sasl.jaas.config"));
        }
        System.out.println("module=" + admin.get("sasl.jaas.config").toString().split("\\s+")[0]);
        try (Admin client = Admin.create(admin)) {
            String id = client.describeCluster().clusterId().get(20, TimeUnit.SECONDS);
            System.out.println("login=ok");
            System.out.println("clusterId=" + id);
        } catch (Throwable t) {
            Throwable root = t;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            System.out.println("login=failed");
            System.out.println("error=" + root.getClass().getSimpleName() + ": " + root.getMessage());
        }
    }
}
