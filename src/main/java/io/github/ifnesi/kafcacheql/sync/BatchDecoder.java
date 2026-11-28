package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.transform.RecordTransforms;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.schema.RecordDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/** One single-threaded decoder and reader per consumer owner; registry caches are shared. */
final class BatchDecoder implements AutoCloseable {
    private final Connection reader;
    private final RecordDecoder decoder;
    private final RecordTransforms transforms;

    BatchDecoder(ClientSettings settings, Connection reader, RecordDecoder.RegistrySource schemas, RecordTransforms transforms) {
        this.transforms = transforms;
        this.reader = reader;
        decoder = new RecordDecoder(settings, reader, schemas);
    }

    record Decoded(List<SyncBatch.Record> records, long fetched) {}
    Decoded decode(String topic, ConsumerRecords<byte[], byte[]> records, long end,
                                  Format key, Format value) throws SQLException {
        List<SyncBatch.Record> decoded = new ArrayList<>(records.count());
        long fetched = 0;
        // Finish the poll even after cancellation; the caller drops it, retaining fetched schemas.
        for (var record : records) {
            if (record.offset() >= end) break;
            fetched++;
            var result = transforms.apply(record, decoder.decode(topic, record.headers(), record.key(), record.value(), key, value));
            if (result != null) decoded.add(result);
        }
        return new Decoded(decoded, fetched);
    }

    @Override public void close() throws SQLException {
        try (reader; decoder) { /* Deserializers close before their reader. */ }
    }
}
