package io.github.ifnesi.kafcacheql.kafka;

import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

/**
 * What the driver needs to know about a topic, resolved from the broker's configs
 * and the {@code topic.<name>.*} overrides.
 *
 * @param configsFromBroker false when DescribeConfigs was denied and the fallbacks in {@link #resolve} apply
 */
public record TopicInfo(
        String name,
        Uuid topicId,
        int partitions,
        Mode mode,
        long deleteRetentionMs,
        TimestampType timestampType,
        boolean configsFromBroker) {

    /** Kafka's default delete.retention.ms: 24 hours. */
    public static final long DEFAULT_DELETE_RETENTION_MS = 86_400_000L;

    static final String CLEANUP_POLICY = "cleanup.policy";
    static final String DELETE_RETENTION_MS = "delete.retention.ms";
    static final String TIMESTAMP_TYPE = "message.timestamp.type";

    /**
     * @param brokerConfigs the topic's configs, or empty when the driver may not read them
     */
    static TopicInfo resolve(String name, Uuid topicId, int partitions, Optional<Map<String, String>> brokerConfigs,
                             DriverConfig config) throws SQLException {
        TopicSettings overrides = config.topic(name);
        if (brokerConfigs.isPresent()) {
            Map<String, String> c = brokerConfigs.get();
            Mode fromPolicy = c.getOrDefault(CLEANUP_POLICY, "delete").contains("compact") ? Mode.COMPACTED : Mode.APPEND;
            long deleteRetention = Optional.ofNullable(c.get(DELETE_RETENTION_MS))
                    .map(Long::parseLong).orElse(DEFAULT_DELETE_RETENTION_MS);
            TimestampType type = "LogAppendTime".equals(c.get(TIMESTAMP_TYPE))
                    ? TimestampType.LOG_APPEND_TIME : TimestampType.CREATE_TIME;
            return new TopicInfo(name, topicId, partitions, overrides.mode().orElse(fromPolicy), deleteRetention, type, true);
        }
        Mode mode = overrides.mode().orElseThrow(() -> ConfigErrors.problem("The configs of topic " + name
                + " can't be read (no DescribeConfigs permission): set topic." + name + ".mode to append or compacted"));
        // CreateTime is the cautious guess: its timestamp upper bounds never limit the fetch.
        return new TopicInfo(name, topicId, partitions, mode,
                overrides.deleteRetentionMs().orElse(DEFAULT_DELETE_RETENTION_MS), TimestampType.CREATE_TIME, false);
    }
}
