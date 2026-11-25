package io.github.ifnesi.kafcacheql.sync;

import java.sql.Connection;
import java.sql.SQLException;

/** 0006b's handoff: invoked on the worker under BEGIN IMMEDIATE after the generation check.
 * Implementations re-read ranges/floors, store only eligible rows, merge processed ranges, store schema
 * copies, refresh the view, and advance caught_up_at_ms only at the captured end. They must not commit,
 * retain the batch, or contact Kafka/Schema Registry. A thrown exception rolls back the whole batch.
 */
@FunctionalInterface
public interface BatchWriter {
    void write(Connection writer, SyncBatch batch) throws SQLException;
}
