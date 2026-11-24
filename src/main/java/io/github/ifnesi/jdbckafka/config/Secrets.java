package io.github.ifnesi.jdbckafka.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Which client settings hold credentials: those Kafka's and Confluent's own definitions type
 * as PASSWORD, and, for settings neither defines, any whose name looks like a credential.
 */
public final class Secrets {

    public static final String HIDDEN = "[hidden]";

    private static final Set<String> PASSWORD_KEYS = Stream.of(
                    ConsumerConfig.configDef(), AdminClientConfig.configDef(), ProducerConfig.configDef(),
                    AbstractKafkaSchemaSerDeConfig.baseConfigDef())
            .flatMap(d -> d.configKeys().values().stream())
            .filter(k -> k.type == ConfigDef.Type.PASSWORD)
            .map(k -> k.name)
            .collect(Collectors.toUnmodifiableSet());

    private static final Pattern LOOKS_SECRET = Pattern.compile(
            "(?i).*(password|passphrase|secret|jaas|user\\.info|token|credential|private\\.key|keystore\\.key).*");

    private Secrets() {}

    public static boolean isSecret(String key) {
        return PASSWORD_KEYS.contains(key) || LOOKS_SECRET.matcher(key).matches();
    }

    /** A copy for toString() and logs: secret values replaced by {@value #HIDDEN}. */
    public static Map<String, String> masked(Map<String, ?> settings) {
        Map<String, String> out = new TreeMap<>();
        settings.forEach((k, v) -> out.put(k, isSecret(k) ? HIDDEN : String.valueOf(v)));
        return out;
    }

    /** {@code text} with every secret value of {@code settings} replaced by {@value #HIDDEN}. */
    public static String redact(String text, Map<String, ?> settings) {
        String out = text;
        for (Map.Entry<String, ?> e : settings.entrySet()) {
            String value = e.getValue() == null ? "" : e.getValue().toString();
            if (isSecret(e.getKey()) && !value.isEmpty()) {
                out = out.replace(value, HIDDEN);
            }
        }
        return out;
    }
}
