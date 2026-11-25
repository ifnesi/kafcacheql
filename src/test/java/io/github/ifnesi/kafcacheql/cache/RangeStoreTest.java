package io.github.ifnesi.kafcacheql.cache;

import static io.github.ifnesi.kafcacheql.cache.CacheStoreTest.query;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeStoreTest {
    @TempDir
    Path dir;

    private static void topic(Connection writer) throws Exception {
        try (Statement s = writer.createStatement()) {
            s.execute("""
                    INSERT INTO "$topics" (topic, view_name, topic_uuid, mode, partition_count,
                                           key_format, value_format, decoder_version)
                    VALUES ('t', 't', 'uuid', 'append', 2, 'auto', 'auto', 1)
                    """);
        }
    }

    /** 0004b acceptance: "randomised tests of add and subtract against a naive set-of-offsets model".
     * PLAN §6.3: "cached offset ranges: half-open [start, end), kept merged"; §10: "trims $ranges below it".
     */
    @Test
    void randomisedAddSubtractAndRemoveBelowMatchOffsets() throws Exception {
        try (CacheStore cache = CacheStore.open(dir.resolve("ranges.db"), Optional.of("A"));
             Statement s = cache.writer().createStatement()) {
            Connection w = cache.writer();
            topic(w);
            Set<Long> cached = new HashSet<>();
            Random random = new Random(0x0004b);
            for (int step = 0; step < 500; step++) {
                long start = random.nextInt(97);
                long end = start + random.nextInt(97 - (int) start);
                s.execute("BEGIN IMMEDIATE");
                if (step % 7 == 6) {
                    RangeStore.removeBelow(w, 1, 0, start);
                    cached.removeIf(offset -> offset < start);
                } else {
                    RangeStore.add(w, 1, 0, start, end);
                    for (long offset = start; offset < end; offset++) {
                        cached.add(offset);
                    }
                }
                s.execute("COMMIT");
                assertEquals(rows(cached), query(w, """
                        SELECT start_offset, end_offset FROM "$ranges"
                        WHERE topic_ref = 1 AND partition_no = 0 ORDER BY start_offset
                        """), "merged persisted ranges, step " + step);
                assertEquals(cached, offsets(RangeStore.load(w, 1, 0)), "loaded ranges, step " + step);

                RangeSet<Long> needed = TreeRangeSet.create();
                Set<Long> missing = new HashSet<>();
                for (int window = 0; window < 3; window++) {
                    long from = random.nextInt(97);
                    long to = from + random.nextInt(97 - (int) from);
                    needed.add(Range.closedOpen(from, to));
                    for (long offset = from; offset < to; offset++) {
                        if (!cached.contains(offset)) {
                            missing.add(offset);
                        }
                    }
                }
                Set<Long> original = offsets(needed);
                assertEquals(missing, offsets(RangeStore.missing(w, 1, 0, needed)), "subtract, step " + step);
                assertEquals(original, offsets(needed), "subtract preserves the requested windows");
                assertEquals(original, offsets(RangeStore.missing(w, 1, 1, needed)), "partition isolation");
            }
            // Touching, bridging, contained, duplicate and empty intervals, plus offsets beyond int/exact double range.
            s.execute("BEGIN IMMEDIATE");
            for (long[] interval : new long[][] {{0, 10}, {20, 30}, {10, 20}, {5, 25}, {0, 30}, {30, 30}}) {
                RangeStore.add(w, 1, 1, interval[0], interval[1]);
            }
            RangeStore.add(w, 1, 1, Long.MAX_VALUE - 10, Long.MAX_VALUE);
            RangeStore.removeBelow(w, 1, 1, 15);
            s.execute("COMMIT");
            assertEquals(List.of("15|30", (Long.MAX_VALUE - 10) + "|" + Long.MAX_VALUE), query(w, """
                    SELECT start_offset, end_offset FROM "$ranges"
                    WHERE topic_ref = 1 AND partition_no = 1 ORDER BY start_offset
                    """));
            RangeSet<Long> high = TreeRangeSet.create();
            high.add(Range.closedOpen(Long.MAX_VALUE - 15, Long.MAX_VALUE));
            assertEquals(Set.of(Range.closedOpen(Long.MAX_VALUE - 15, Long.MAX_VALUE - 10)),
                    RangeStore.missing(w, 1, 1, high).asRanges());
        }
    }

    /** 0004b acceptance: "a persistence round trip". PLAN §6.3: "$ranges" stores the cached ranges. */
    @Test
    void rangesSurviveClosingAndReopeningTheCache() throws Exception {
        Path file = dir.resolve("persist.db");
        try (CacheStore cache = CacheStore.open(file, Optional.of("A")); Statement s = cache.writer().createStatement()) {
            topic(cache.writer());
            s.execute("BEGIN IMMEDIATE");
            RangeStore.add(cache.writer(), 1, 0, 10, 20);
            RangeStore.add(cache.writer(), 1, 0, 20, 40);
            RangeStore.add(cache.writer(), 1, 0, 50, 60);
            RangeStore.add(cache.writer(), 1, 1, 0, 5);
            RangeStore.removeBelow(cache.writer(), 1, 0, 15);
            s.execute("COMMIT");
        }
        try (CacheStore cache = CacheStore.open(file, Optional.empty()); Connection reader = cache.newReader()) {
            assertEquals(Set.of(Range.closedOpen(15L, 40L), Range.closedOpen(50L, 60L)),
                    RangeStore.load(reader, 1, 0).asRanges());
            assertEquals(Set.of(Range.closedOpen(0L, 5L)), RangeStore.load(reader, 1, 1).asRanges());
            RangeSet<Long> needed = TreeRangeSet.create();
            needed.add(Range.closedOpen(10L, 65L));
            assertEquals(Set.of(Range.closedOpen(10L, 15L), Range.closedOpen(40L, 50L), Range.closedOpen(60L, 65L)),
                    RangeStore.missing(reader, 1, 0, needed).asRanges());
            assertEquals(List.of("15|40", "50|60"), query(reader, """
                    SELECT start_offset, end_offset FROM "$ranges"
                    WHERE topic_ref = 1 AND partition_no = 0 ORDER BY start_offset
                    """));
        }
    }

    private static Set<Long> offsets(RangeSet<Long> ranges) {
        Set<Long> offsets = new HashSet<>();
        for (Range<Long> range : ranges.asRanges()) {
            for (long offset = range.lowerEndpoint(); offset < range.upperEndpoint(); offset++) {
                offsets.add(offset);
            }
        }
        return offsets;
    }

    /** Independent oracle: scan the small offset domain, collecting each contiguous run. */
    private static List<String> rows(Set<Long> offsets) {
        List<String> rows = new ArrayList<>();
        for (long offset = 0; offset < 97; offset++) {
            if (offsets.contains(offset)) {
                long start = offset;
                while (offsets.contains(offset + 1)) {
                    offset++;
                }
                rows.add(start + "|" + (offset + 1));
            }
        }
        return rows;
    }
}
