package io.github.ifnesi.kafcacheql.sync;

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

    BatchDecoder(ClientSettings settings, Connection reader, RecordDecoder.RegistrySource schemas) {
        this.reader = reader;
        decoder = new RecordDecoder(settings, reader, schemas);
    }

    List<SyncBatch.Record> decode(String topic, ConsumerRecords<byte[], byte[]> records, long end,
                                  Format key, Format value) throws SQLException {
        List<SyncBatch.Record> decoded = new ArrayList<>(records.count());
        // Finish the poll even after cancellation; the caller drops it, retaining fetched schemas.
        for (var record : records) {
            if (record.offset() >= end) break;
            decoded.add(new SyncBatch.Record(record.offset(), record.timestamp(), record.key(), record.headers(),
                    decoder.decode(topic, record.headers(), record.key(), record.value(), key, value)));
        }
        return decoded;
    }

    @Override public void close() throws SQLException {
        try (reader; decoder) { /* Deserializers close before their reader. */ }
    }
}
