package io.github.ifnesi.jdbckafka.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.jdbckafka.config.DriverConfig;
import io.github.ifnesi.jdbckafka.config.Mode;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The DescribeConfigs-denied fallbacks, simulated. */
class TopicInfoTest {

    private static final Uuid ID = Uuid.randomUuid();

    private static DriverConfig config(String... kv) throws SQLException {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return DriverConfig.load("jdbc:kafka://h:1", p);
    }

    private static TopicInfo fromBroker(Map<String, String> configs, String... kv) throws SQLException {
        return TopicInfo.resolve("t", ID, 3, Optional.of(configs), config(kv));
    }

    @ParameterizedTest
    @CsvSource({"delete,APPEND", "compact,COMPACTED", "'compact,delete',COMPACTED", "'delete,compact',COMPACTED", "'',APPEND"})
    void modeFromCleanupPolicy(String policy, Mode mode) throws SQLException {
        assertEquals(mode, fromBroker(Map.of("cleanup.policy", policy)).mode());
    }

    @Test
    void brokerConfigs() throws SQLException {
        TopicInfo t = fromBroker(Map.of("cleanup.policy", "compact", "delete.retention.ms", "1000",
                "message.timestamp.type", "LogAppendTime"));
        assertEquals("t", t.name());
        assertEquals(ID, t.topicId());
        assertEquals(3, t.partitions());
        assertEquals(1000, t.deleteRetentionMs());
        assertEquals(TimestampType.LOG_APPEND_TIME, t.timestampType());
        assertTrue(t.configsFromBroker());
        assertEquals(TimestampType.CREATE_TIME, fromBroker(Map.of()).timestampType());
        assertEquals(TopicInfo.DEFAULT_DELETE_RETENTION_MS, fromBroker(Map.of()).deleteRetentionMs());
    }

    @Test
    void theModeOverrideWinsOverTheBroker() throws SQLException {
        assertEquals(Mode.APPEND, fromBroker(Map.of("cleanup.policy", "compact"), "topic.t.mode", "append").mode());
    }

    @Test
    void theDeleteRetentionOverrideIsOnlyAFallback() throws SQLException {
        assertEquals(1000, fromBroker(Map.of("delete.retention.ms", "1000"), "topic.t.delete.retention.ms", "5").deleteRetentionMs());
    }

    @Test
    void deniedConfigsNeedTheModeOverride() throws SQLException {
        SQLException e = assertThrows(SQLException.class,
                () -> TopicInfo.resolve("orders.v1", ID, 1, Optional.empty(), config()));
        assertTrue(e.getMessage().contains("topic.orders.v1.mode"), e.getMessage());
    }

    @Test
    void deniedConfigsUseTheOverridesOrKafkasDefaults() throws SQLException {
        TopicInfo t = TopicInfo.resolve("t", ID, 1, Optional.empty(), config("topic.t.mode", "compacted"));
        assertEquals(Mode.COMPACTED, t.mode());
        assertEquals(TopicInfo.DEFAULT_DELETE_RETENTION_MS, t.deleteRetentionMs(), "Kafka's 24 h default");
        assertEquals(TimestampType.CREATE_TIME, t.timestampType(), "the cautious guess");
        assertFalse(t.configsFromBroker());

        TopicInfo u = TopicInfo.resolve("t", ID, 1, Optional.empty(),
                config("topic.t.mode", "compacted", "topic.t.delete.retention.ms", "3600000"));
        assertEquals(3_600_000, u.deleteRetentionMs());
    }
}
