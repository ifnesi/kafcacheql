package io.github.ifnesi.kafcacheql.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Secrets;
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

    private static final Set<String> KNOWN = DriverLoader.call(() -> Stream.of(
                    ConsumerConfig.configNames(), AdminClientConfig.configNames(), ProducerConfig.configNames(),
                    AbstractKafkaSchemaSerDeConfig.baseConfigDef().names())
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet()));

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

    /**
     * Credentials written into schema.registry.url (Confluent reads them with basic.auth.credentials.source
     * URL, its default) move to basic.auth.user.info: the clients log their URL settings in full but hide
     * basic.auth.user.info. They are read by java.net.URL, as Confluent reads them (Secrets.urlCredentials),
     * and taken as written, as Confluent takes URL.getUserInfo(). Every '@' in the setting must end a URL's
     * credentials: any other, which would reach the clients' logs as written, is refused. basic.auth.user.info
     * applies to every URL, so a list where only some URLs carry credentials is refused too: moving them would
     * send them to URLs that had none.
     */
    private static Map<String, String> registryCredentialsFromUrl(Map<String, String> settings) throws SQLException {
        String urls = settings.get(REGISTRY_URL);
        if (urls == null || !urls.contains("@")) {
            return settings;
        }
        Set<String> credentials = new java.util.LinkedHashSet<>();
        List<String> withoutCredentials = new java.util.ArrayList<>();
        int bare = 0;
        for (String url : urls.split(",")) {
            String entry = url.trim();
            if (entry.indexOf('@') < 0) {
                bare++;
                withoutCredentials.add(entry);
                continue;
            }
            Secrets.UrlCredentials parsed = Secrets.urlCredentials(entry).orElse(null);
            if (parsed != null && parsed.ambiguous()) {
                throw ConfigErrors.problem("schema.registry.url has more than one '@' before a host: percent-encode "
                        + "an '@' inside credentials as %40, or set them in basic.auth.user.info");
            }
            if (parsed == null || parsed.userInfo() == null || parsed.withoutUserInfo().indexOf('@') >= 0) {
                throw ConfigErrors.problem("schema.registry.url has an '@' that doesn't end a URL's credentials "
                        + "(scheme://user:password@host): percent-encode '@', '/', '?' and '#' inside credentials, "
                        + "or set them in basic.auth.user.info");
            }
            credentials.add(parsed.userInfo());
            withoutCredentials.add(parsed.withoutUserInfo());
        }
        if (credentials.isEmpty()) {
            return settings;
        }
        if (credentials.size() > 1) {
            throw ConfigErrors.problem("schema.registry.url holds different credentials in its URLs: set them once, "
                    + "in basic.auth.user.info");
        }
        if (bare > 0) {
            throw ConfigErrors.problem("schema.registry.url gives credentials to some of its URLs only: give every URL "
                    + "the same credentials, or set them once in basic.auth.user.info");
        }
        String userInfo = credentials.iterator().next();
        String source = settings.get(CREDENTIALS_SOURCE);
        if (source != null && !source.equals("URL") && !source.equals("USER_INFO")) {
            throw ConfigErrors.problem("schema.registry.url holds credentials, but basic.auth.credentials.source is "
                    + source + ": keep the credentials in one place");
        }
        for (String key : List.of(USER_INFO, PREFIXED_USER_INFO)) {
            if (settings.containsKey(key) && !settings.get(key).equals(userInfo)) {
                throw ConfigErrors.problem("schema.registry.url and " + key + " hold different credentials: "
                        + "keep them in " + key + " only");
            }
        }
        Map<String, String> out = new LinkedHashMap<>(settings);
        out.put(REGISTRY_URL, String.join(",", withoutCredentials));
        out.put(CREDENTIALS_SOURCE, "USER_INFO");
        out.put(USER_INFO, userInfo);
        return out;
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
            Map<String, Object> all = new TreeMap<>(given);
            all.putAll(schemaRegistry);
            throw ConfigErrors.problem("Invalid client setting: " + Secrets.redact(e.getMessage(), all));
        }
    }
}
