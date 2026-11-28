import io.github.ifnesi.kafcacheql.cli.Main;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.shaded.org.apache.kafka.clients.admin.Admin;
import io.github.ifnesi.kafcacheql.shaded.org.apache.kafka.clients.admin.ListTopicsResult;
import io.github.ifnesi.kafcacheql.shaded.org.apache.kafka.clients.admin.TopicListing;
import io.github.ifnesi.kafcacheql.shaded.org.apache.kafka.common.KafkaFuture;
import io.github.ifnesi.kafcacheql.shaded.org.apache.kafka.common.internals.KafkaFutureImpl;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;

/** Stall only topic-list replies after the real CLI's initial completion load. All other Kafka calls are real. */
public class CliRefreshProbe {
    public static void main(String[] args) throws Exception {
        System.setProperty("slf4j.provider", "org.slf4j.helpers.NOP_FallbackServiceProvider");
        System.setProperty("slf4j.internal.verbosity", "ERROR");
        Path signals = Path.of(args[1]);
        try (var lease = KafkaClients.acquire(DriverConfig.load(args[0], null))) {
            var field = KafkaClients.class.getDeclaredField("admin");
            field.setAccessible(true);
            Admin original = (Admin) field.get(lease.clients());
            field.set(lease.clients(), Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[]{Admin.class},
                    (self, method, arguments) -> {
                        try {
                            Object result = method.invoke(original, arguments);
                            if (!method.getName().equals("listTopics") || !Files.exists(signals.resolve("stall-refresh"))) return result;
                            boolean background = Thread.currentThread().getName().equals("kafcacheql-completions");
                            var pending = new KafkaFutureImpl<java.util.Map<String, TopicListing>>();
                            var constructor = ListTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);
                            constructor.setAccessible(true);
                            Files.writeString(signals.resolve(background ? "refresh-stalled" : "sync-stalled"), "");
                            Thread.ofPlatform().daemon(true).start(() -> {
                                try {
                                    while (!Files.exists(signals.resolve("release-refresh"))) Thread.sleep(20);
                                    pending.complete(((ListTopicsResult) result).namesToListings().get());
                                } catch (Exception failure) { pending.completeExceptionally(failure); }
                            });
                            return constructor.newInstance(pending);
                        } catch (InvocationTargetException failure) { throw failure.getCause(); }
                    }));
            Main.main(new String[]{"--mode", "json", args[0]});
        }
    }
}
