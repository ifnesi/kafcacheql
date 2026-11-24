import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Run by DriverManagerIT in a JVM of its own, with only the shaded JAR and a different kafka-clients on the
 * class path: {@code java -cp <shaded JAR>:<kafka-clients> DriverManagerProbe.java <url>}.
 * Prints key=value lines for the test to check.
 */
public class DriverManagerProbe {

    public static void main(String[] args) throws Exception {
        Driver driver = DriverManager.getDriver(args[0]);
        System.out.println("driver=" + driver.getClass().getName());
        System.out.println("foreign=" + driver.connect("jdbc:sqlite::memory:", new Properties()));
        System.out.println("kafka=" + jarOf("org.apache.kafka.clients.consumer.KafkaConsumer"));
        System.out.println("shaded=" + jarOf("io.github.ifnesi.jdbckafka.shaded.org.apache.kafka.clients.consumer.KafkaConsumer"));
        try {
            DriverManager.getDriver("jdbc:sqlite::memory:");
            System.out.println("sqlite=registered");
        } catch (SQLException e) {
            System.out.println("sqlite=not registered");
        }
    }

    private static String jarOf(String className) throws ClassNotFoundException {
        String path = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();
        return path.substring(path.lastIndexOf('/') + 1);
    }
}
