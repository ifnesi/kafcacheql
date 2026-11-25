package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.schema.RecordDecoder;
import java.util.List;
import org.apache.kafka.common.header.Headers;

/** A bounded handoff to 0006b. No Kafka operation occurs inside the handoff's write transaction.
 * Records are in offset order in one partition. The final end is position clipped to the requested end;
 * intermediate ends follow the last included record, never the later records still buffered by poll.
 */
public record SyncBatch(SyncPlan plan, long generation, int partition, long start, long end,
                        boolean reachedEnd, List<Record> records) {
    public record Record(long offset, long timestamp, byte[] key, Headers headers, RecordDecoder.Decoded decoded) {}
}
