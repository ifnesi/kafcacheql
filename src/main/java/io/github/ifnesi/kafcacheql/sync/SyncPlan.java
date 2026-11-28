package io.github.ifnesi.kafcacheql.sync;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;
import static io.github.ifnesi.kafcacheql.util.ControlCharacters.escape;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.kafka.EndOffsets;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

/** Read-side observation: generation was captured before all the Kafka calls it guards. */
public record SyncPlan(TopicInfo topic, OptionalLong generation, boolean deleted,
                       Map<Integer, ImmutableRangeSet<Long>> windows, EndOffsets ends,
                       Map<Integer, Long> logStarts, List<SQLWarning> warnings, Source source) {
    public enum Source { KAFKA, RECENT, OFFLINE }
    public boolean checked() { return source == Source.KAFKA; }
    public SyncPlan {
        windows = Map.copyOf(windows);
        logStarts = Map.copyOf(logStarts);
        warnings = List.copyOf(warnings);
    }

    public static SyncPlan create(TopicInfo topic, OptionalLong generation, boolean deleted,
                                 Map<Integer, ? extends RangeSet<Long>> wanted, EndOffsets ends, Map<Integer, Long> starts) {
        return create(topic, generation, deleted, wanted, ends, starts, Source.KAFKA);
    }

    public static SyncPlan create(TopicInfo topic, OptionalLong generation, boolean deleted,
            Map<Integer, ? extends RangeSet<Long>> wanted, EndOffsets ends, Map<Integer, Long> starts, Source source) {
        Map<Integer, ImmutableRangeSet<Long>> windows = new TreeMap<>();
        var warnings = new ArrayList<SQLWarning>();
        for (int partition = 0; partition < topic.partitions(); partition++) {
            Range<Long> available = Range.closedOpen(starts.get(partition), ends.offsets().get(partition));
            RangeSet<Long> ranges = wanted == null || topic.mode() == Mode.COMPACTED
                    ? ImmutableRangeSet.of(available) : wanted.get(partition);
            if (ranges != null) {
                if (source != Source.OFFLINE && topic.mode() == Mode.APPEND && !ranges.subRangeSet(Range.lessThan(starts.get(partition))).isEmpty()) {
                    warnings.add(new SQLWarning(sentence(escape("Topic " + topic.name() + " partition " + partition
                            + ": requested data below offset " + starts.get(partition) + " has expired")), "01000"));
                }
                windows.put(partition, ImmutableRangeSet.copyOf(source == Source.OFFLINE ? ranges : ranges.subRangeSet(available)));
            }
        }
        return new SyncPlan(topic, generation, deleted, windows, ends, starts, warnings, source);
    }

    /** Shares one Kafka observation between references and the union sent to the worker. */
    public SyncPlan withWindows(Map<Integer, ? extends RangeSet<Long>> wanted) {
        return topic.mode() == Mode.COMPACTED ? this : create(topic, generation, deleted, wanted, ends, logStarts, source);
    }
}
