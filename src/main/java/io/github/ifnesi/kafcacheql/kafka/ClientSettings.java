package io.github.ifnesi.kafcacheql.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Secrets;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
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

    /** Schema Registry client settings that no Confluent definition lists: the client reads them raw. */
    private static final Set<String> REGISTRY_CLIENT_ONLY = Set.of(
            SchemaRegistryClientConfig.MAX_RETRIES_CONFIG, SchemaRegistryClientConfig.RETRIES_WAIT_MS_CONFIG,
            SchemaRegistryClientConfig.RETRIES_MAX_WAIT_MS_CONFIG, SchemaRegistryClientConfig.LATEST_CACHE_TTL_CONFIG,
            SchemaRegistryClientConfig.MISSING_CACHE_SIZE_CONFIG, SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG,
            SchemaRegistryClientConfig.MISSING_VERSION_CACHE_TTL_CONFIG,
            SchemaRegistryClientConfig.MISSING_SCHEMA_CACHE_TTL_CONFIG, SchemaRegistryClientConfig.URL_RANDOMIZE,
            SchemaRegistryClientConfig.USE_APACHE_HTTP_CLIENT);

    private static final Set<String> KNOWN = DriverLoader.call(() -> Stream.of(
                    ConsumerConfig.configNames(), AdminClientConfig.configNames(), ProducerConfig.configNames(),
                    AbstractKafkaSchemaSerDeConfig.baseConfigDef().names(), REGISTRY_CLIENT_ONLY)
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet()));

    /** Prefixes of Schema Registry client and Kafka settings that no definition lists one by one. */
    private static final List<String> KNOWN_PREFIXES = List.of(
            "schema.registry.", "basic.auth.", "bearer.auth.", "proxy.", "http.", "config.providers.",
            AbstractKafkaSchemaSerDeConfig.REQUEST_HEADER_PREFIX);

    private final Map<String, Object> consumer;
    private final Map<String, Object> admin;
    private final Map<String, Object> schemaRegistry;

    private ClientSettings(Map<String, Object> consumer, Map<String, Object> admin, Map<String, Object> schemaRegistry) {
        this.consumer = Collections.unmodifiableMap(consumer);
        this.admin = Collections.unmodifiableMap(admin);
        this.schemaRegistry = Collections.unmodifiableMap(schemaRegistry);
    }

    /** Validation loads the classes settings name, so it runs with the driver's class loader (DriverLoader). */
    public static ClientSettings of(DriverConfig config) throws SQLException {
        return DriverLoader.call(() -> build(config));
    }

    private static ClientSettings build(DriverConfig config) throws SQLException {
        Map<String, String> given = config.clientSettings();
        Map<String, String> settings = new LinkedHashMap<>(ShadedNames.rewrite(given));
        if (given.containsKey(JaasSettings.KEY)) {
            settings.put(JaasSettings.KEY, JaasSettings.prepare(given.get(JaasSettings.KEY), ShadedNames.PREFIX));
        }
        settings = registryCredentialsFromUrl(settings);
        checkFixed(settings);
        for (String key : List.of(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
                AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID)) {
            if (settings.containsKey(key)) {
                throw ConfigErrors.problem(key + " can't be set: KafCacheQL decodes each key and value "
                        + "with the schema its own bytes name; remove it");
            }
        }
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
                throw ConfigErrors.problem(f.getKey() + " can't be set: KafCacheQL uses no consumer group and keeps "
                        + "its offsets in its cache");
            }
            if (!value.trim().equals(f.getValue())) {
                throw ConfigErrors.problem(f.getKey() + " is fixed at " + f.getValue()
                        + " by KafCacheQL; remove it");
            }
        }
    }

    private static void warnUnknown(Set<String> keys) {
        for (String key : keys) {
            boolean known = KNOWN.contains(key) || KNOWN_PREFIXES.stream().anyMatch(key::startsWith)
                    || key.endsWith(".class") || key.endsWith(".classes");
            if (!known) {
                log.warn("Setting '{}' isn't a KafCacheQL property or a known Kafka or Schema Registry client "
                        + "setting; check it for a typo", key);
            }
        }
    }

    private static final String REGISTRY_URL = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
    private static final String CREDENTIALS_SOURCE = "basic.auth.credentials.source";
    private static final String USER_INFO = "basic.auth.user.info";
    private static final String PREFIXED_USER_INFO = "schema.registry.basic.auth.user.info";

    /** Move URL credentials to a PASSWORD setting before Confluent can log its configuration. */
    @SuppressWarnings("deprecation") // The registry client also uses URL(String).
    private static Map<String, String> registryCredentialsFromUrl(Map<String, String> settings) throws SQLException {
        String urls = settings.get(REGISTRY_URL);
        if (urls == null || !urls.contains("@")) {
            return settings;
        }
        List<String> clean = new ArrayList<>();
        String credentials = null;
        for (String entry : urls.split(",", -1)) {
            try {
                URL url = new URL(entry.trim());
                String userInfo = url.getUserInfo();
                // One shared basic.auth.user.info must preserve every URL's original credentials.
                if (userInfo == null || entry.chars().filter(c -> c == '@').count() != 1
                        || credentials != null && !credentials.equals(userInfo)) {
                    throw registryCredentialsError();
                }
                credentials = userInfo;
                clean.add(url.getProtocol() + "://" + url.getAuthority().substring(url.getAuthority().indexOf('@') + 1)
                        + url.getFile() + (url.getRef() == null ? "" : "#" + url.getRef()));
            } catch (MalformedURLException e) {
                throw registryCredentialsError();
            }
        }
        String source = settings.getOrDefault(CREDENTIALS_SOURCE, "URL");
        if (!source.equals("URL") && !source.equals("USER_INFO")) {
            throw registryCredentialsError();
        }
        for (String key : List.of(USER_INFO, PREFIXED_USER_INFO)) {
            if (settings.containsKey(key) && !settings.get(key).equals(credentials)) {
                throw registryCredentialsError();
            }
        }
        Map<String, String> out = new LinkedHashMap<>(settings);
        out.put(REGISTRY_URL, String.join(",", clean));
        out.put(CREDENTIALS_SOURCE, "USER_INFO");
        out.put(USER_INFO, credentials);
        return out;
    }

    private static SQLException registryCredentialsError() {
        return ConfigErrors.problem("Invalid credentials in schema.registry.url: use URLs without credentials "
                + "and set basic.auth.user.info with basic.auth.credentials.source=USER_INFO");
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
            var parsed = ConsumerConfig.configDef().parse(consumer);
            for (var entry : Map.of(
                    "sasl.login.callback.handler.class", org.apache.kafka.common.security.auth.AuthenticateCallbackHandler.class,
                    "sasl.client.callback.handler.class", org.apache.kafka.common.security.auth.AuthenticateCallbackHandler.class,
                    "sasl.login.class", org.apache.kafka.common.security.auth.Login.class,
                    "ssl.engine.factory.class", org.apache.kafka.common.security.auth.SslEngineFactory.class).entrySet()) {
                var configured = (Class<?>) parsed.get(entry.getKey());
                if (configured != null && !entry.getValue().isAssignableFrom(configured))
                    throw ConfigErrors.problem(entry.getKey() + " must name a class that implements " + entry.getValue().getSimpleName());
            }
            AdminClientConfig.configDef().parse(admin);
            if (schemaRegistry.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
                AbstractKafkaSchemaSerDeConfig.baseConfigDef().parse(schemaRegistry);
            }
        } catch (ConfigException e) {
            Map<String, Object> all = new TreeMap<>(given);
            all.putAll(schemaRegistry);
            throw ConfigErrors.problem("Invalid client setting: " + Secrets.redact(e.getMessage(), all));
        }
    }
}
