package io.github.ifnesi.jdbckafka.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.ifnesi.jdbckafka.config.ConfigErrors;
import io.github.ifnesi.jdbckafka.config.DriverConfig;
import io.github.ifnesi.jdbckafka.config.Secrets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The settings each client receives. Every property that isn't the driver's own reaches
 * the clients unchanged, except class names rewritten for the shaded JAR; each Kafka client gets only the
 * keys it recognises; the consumer's fixed settings can't be overridden. client.id is set by KafkaClients.
 */
public final class ClientSettings {

    private static final Logger log = LoggerFactory.getLogger(ClientSettings.class);

    private static final String GROUP_ID = ConsumerConfig.GROUP_ID_CONFIG;

    /** The fixed consumer settings and their values. group.id must be absent. */
    static final Map<String, String> FIXED = fixed();

    private static final Set<String> KNOWN = Stream.of(
                    ConsumerConfig.configNames(), AdminClientConfig.configNames(), ProducerConfig.configNames(),
                    AbstractKafkaSchemaSerDeConfig.baseConfigDef().names())
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet());

    /** Prefixes of Schema Registry client and Kafka settings that no definition lists one by one. */
    private static final List<String> KNOWN_PREFIXES = List.of(
            "schema.registry.", "basic.auth.", "bearer.auth.", "proxy.", "http.", "config.providers.");

    private final Map<String, Object> consumer;
    private final Map<String, Object> admin;
    private final Map<String, Object> schemaRegistry;

    private ClientSettings(Map<String, Object> consumer, Map<String, Object> admin, Map<String, Object> schemaRegistry) {
        this.consumer = Collections.unmodifiableMap(consumer);
        this.admin = Collections.unmodifiableMap(admin);
        this.schemaRegistry = Collections.unmodifiableMap(schemaRegistry);
    }

    public static ClientSettings of(DriverConfig config) throws SQLException {
        Map<String, String> given = config.clientSettings();
        Map<String, String> settings = ShadedNames.rewrite(given);
        checkFixed(settings);
        warnUnknown(given.keySet());

        Map<String, Object> consumer = only(settings, ConsumerConfig.configNames());
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumer.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumer.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        Map<String, Object> admin = only(settings, AdminClientConfig.configNames());
        Map<String, Object> schemaRegistry = new TreeMap<>(settings);

        if (!config.offline()) {
            validate(consumer, admin, schemaRegistry, given);
        }
        return new ClientSettings(consumer, admin, schemaRegistry);
    }

    public Map<String, Object> consumer() {
        return consumer;
    }

    public Map<String, Object> admin() {
        return admin;
    }

    /** Every client setting, for Confluent's Schema Registry client and deserializers. */
    public Map<String, Object> schemaRegistry() {
        return schemaRegistry;
    }

    @Override
    public String toString() {
        return "ClientSettings[consumer=" + Secrets.masked(consumer) + ", admin=" + Secrets.masked(admin)
                + ", schemaRegistry=" + Secrets.masked(schemaRegistry) + "]";
    }

    // ------------------------------------------------------------------

    private static Map<String, String> fixed() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(GROUP_ID, null);
        m.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        m.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        m.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        m.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        m.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        m.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return Collections.unmodifiableMap(m);
    }

    /** A fixed setting given with its fixed value is harmless; any other value fails, as does group.id. */
    private static void checkFixed(Map<String, String> settings) throws SQLException {
        for (Map.Entry<String, String> f : FIXED.entrySet()) {
            String value = settings.get(f.getKey());
            if (value == null) {
                continue;
            }
            if (f.getValue() == null) {
                throw ConfigErrors.problem(f.getKey() + " can't be set: jdbc-kafka uses no consumer group and keeps "
                        + "its offsets in its cache");
            }
            if (!value.trim().equals(f.getValue())) {
                throw ConfigErrors.problem(f.getKey() + " is fixed at " + f.getValue()
                        + " by jdbc-kafka; remove it");
            }
        }
    }

    private static void warnUnknown(Set<String> keys) {
        for (String key : keys) {
            boolean known = KNOWN.contains(key) || KNOWN_PREFIXES.stream().anyMatch(key::startsWith)
                    || key.endsWith(".class") || key.endsWith(".classes");
            if (!known) {
                log.warn("Setting '{}' isn't a jdbc-kafka property or a known Kafka or Schema Registry client "
                        + "setting; check it for a typo", key);
            }
        }
    }

    private static Map<String, Object> only(Map<String, String> settings, Set<String> names) {
        Map<String, Object> out = new TreeMap<>();
        settings.forEach((k, v) -> {
            if (names.contains(k)) {
                out.put(k, v);
            }
        });
        return out;
    }

    /** Types and validators, as the clients will check them, before any client exists. */
    private static void validate(Map<String, Object> consumer, Map<String, Object> admin,
                                 Map<String, Object> schemaRegistry, Map<String, String> given) throws SQLException {
        try {
            ConsumerConfig.configDef().parse(consumer);
            AdminClientConfig.configDef().parse(admin);
            if (schemaRegistry.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
                AbstractKafkaSchemaSerDeConfig.baseConfigDef().parse(schemaRegistry);
            }
        } catch (ConfigException e) {
            throw ConfigErrors.problem("Invalid client setting: " + Secrets.redact(e.getMessage(), given));
        }
    }
}
