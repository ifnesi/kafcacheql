package io.github.ifnesi.kafcacheql.sync;

import com.google.common.collect.ImmutableRangeSet;
import io.github.ifnesi.kafcacheql.kafka.EndOffsets;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Read-side observation: generation was captured before all the Kafka calls it guards. */
public record SyncPlan(TopicInfo topic, OptionalLong generation,
                       Map<Integer, ImmutableRangeSet<Long>> windows, EndOffsets ends,
                       Map<Integer, Long> logStarts, List<SQLWarning> warnings) {
    public SyncPlan {
        windows = Map.copyOf(windows);
        logStarts = Map.copyOf(logStarts);
        warnings = List.copyOf(warnings);
    }
}
