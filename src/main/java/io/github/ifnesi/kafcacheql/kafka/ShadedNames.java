package io.github.ifnesi.kafcacheql.kafka;

import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Class names in user settings, rewritten to the shaded JAR's relocated packages. In the shaded JAR,
 * Kafka's and Confluent's classes only exist under the relocation prefix, so a setting naming the original
 * class would fail. sasl.jaas.config is {@link JaasSettings}'s. Unshaded (tests on the plain class path) the
 * prefix is empty and nothing changes.
 */
final class ShadedNames {

    // Built at run time: the shade plugin rewrites string constants that look like relocated packages.
    private static final String KAFKA = String.join(".", "org", "apache", "kafka") + ".";
    private static final String CONFLUENT = String.join(".", "io", "confluent") + ".";

    /** The relocation prefix, e.g. {@code io.github.ifnesi.kafcacheql.shaded.}; empty when unshaded. */
    static final String PREFIX = prefixOf(CommonClientConfigs.class.getName());

    /** Settings typed CLASS by Kafka or Confluent: one class name. */
    private static final Set<String> CLASS_KEYS = keysOfType(ConfigDef.Type.CLASS);

    /** Settings that hold a list of class names. */
    private static final Set<String> CLASS_LIST_KEYS = Stream.concat(
                    keysOfType(ConfigDef.Type.LIST).stream().filter(k -> k.endsWith(".classes")),
                    Stream.of("metric.reporters", "partition.assignment.strategy"))
            .collect(Collectors.toUnmodifiableSet());

    private ShadedNames() {}

    static Map<String, String> rewrite(Map<String, String> settings) {
        return rewrite(settings, PREFIX);
    }

    static Map<String, String> rewrite(Map<String, String> settings, String prefix) {
        Map<String, String> out = new LinkedHashMap<>();
        settings.forEach((key, value) -> out.put(key, rewrite(key, value, prefix)));
        return out;
    }

    static String rewrite(String key, String value, String prefix) {
        if (prefix.isEmpty() || value == null) {
            return value;
        }
        if (key.equals(JaasSettings.KEY)) {
            return value;
        }
        if (CLASS_KEYS.contains(key) || key.endsWith(".class")) {
            return className(value, prefix);
        }
        if (CLASS_LIST_KEYS.contains(key) || key.endsWith(".classes")) {
            return classList(value, prefix);
        }
        return value;
    }

    static String className(String value, String prefix) {
        String name = value.trim();
        if (!relocatable(name)) {
            return value;
        }
        int at = value.indexOf(name);
        return value.substring(0, at) + prefix + value.substring(at);
    }

    static String classList(String value, String prefix) {
        String[] parts = value.split(",", -1);
        for (int i = 0; i < parts.length; i++) {
            parts[i] = className(parts[i], prefix);
        }
        return String.join(",", parts);
    }

    static boolean relocatable(String name) {
        return name.startsWith(KAFKA) || name.startsWith(CONFLUENT);
    }

    static String prefixOf(String kafkaClassName) {
        int at = kafkaClassName.indexOf(KAFKA);
        return at <= 0 ? "" : kafkaClassName.substring(0, at);
    }

    private static Set<String> keysOfType(ConfigDef.Type type) {
        return DriverLoader.call(() -> Stream.of(ConsumerConfig.configDef(), AdminClientConfig.configDef(),
                        ProducerConfig.configDef(), AbstractKafkaSchemaSerDeConfig.baseConfigDef())
                .flatMap(d -> d.configKeys().values().stream())
                .filter(k -> k.type == type)
                .map(k -> k.name)
                .collect(Collectors.toUnmodifiableSet()));
    }
}
