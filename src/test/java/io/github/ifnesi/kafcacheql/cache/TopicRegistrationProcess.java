package io.github.ifnesi.kafcacheql.cache;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Optional;
import java.util.OptionalLong;

/** A separate process for the registration race; stdin releases both competitors together. */
public final class TopicRegistrationProcess {
    private TopicRegistrationProcess() {}

    public static void main(String[] args) throws Exception {
        DriverConfig config = DriverConfig.load("jdbc:kafcacheql://" + args[3], null);
        try (CacheStore cache = CacheStore.open(Path.of(args[0]), Optional.of(args[1]));
             Connection r = cache.newReader(); KafkaClients.Lease lease = KafkaClients.acquire(config)) {
            OptionalLong generation = TopicRegistry.generation(r, args[2]);
            TopicInfo info = lease.clients().describeTopic(args[2], config).orElseThrow();
            System.out.println("READY");
            if (System.in.read() != '\n') {
                throw new IllegalStateException("missing start signal");
            }
            System.out.println("REGISTERING");
            TopicRegistry.Registration registered = TopicRegistry.register(cache, generation, info, TopicSettings.NONE, 1)
                    .orElseThrow();
            System.out.println("RESULT " + registered.id() + "|" + registered.viewName());
        }
    }
}
