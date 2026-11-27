package io.github.ifnesi.kafcacheql.cache;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.schema.RecordDecoder;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.header.Headers;

/** The real BatchWriter handoff: no network calls, transaction ownership, or retained batch state. */
public final class RecordStore {
    private RecordStore() {}

    /** Called after the worker's generation check, under BEGIN IMMEDIATE. */
    public static void write(Connection writer, SyncBatch batch) throws SQLException {
        long from = Math.max(batch.start(), PartitionStore.floor(writer, batch.generation(), batch.partition()));
        var eligible = from < batch.end() ? RangeStore.missing(writer, batch.generation(), batch.partition(),
                ImmutableRangeSet.of(Range.closedOpen(from, batch.end()))) : ImmutableRangeSet.<Long>of();
        boolean compacted = batch.plan().topic().mode() == Mode.COMPACTED;
        String table = StorageDdl.quote(StorageDdl.tableName(batch.generation()));
        Map<String, SchemaStore.StoredSchema> schemas = new HashMap<>();
        Set<SchemaStore.RecordType> keyIds = new HashSet<>(), valueIds = new HashSet<>();
        if (!eligible.isEmpty()) {
            String insert = "INSERT " + (compacted ? "" : "OR IGNORE ") + "INTO " + table + """
                     (_partition,_offset,_timestamp,_key_raw,_key,_key_schema_id,_value,_value_schema_id,_headers,_decode_error,
                      _key_message_index,_value_message_index)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """;
            if (compacted) insert += """
                    ON CONFLICT(_partition, _key_raw) DO UPDATE SET
                        _offset=excluded._offset, _timestamp=excluded._timestamp,
                        _key=excluded._key, _key_schema_id=excluded._key_schema_id,
                        _key_message_index=excluded._key_message_index,
                        _value=excluded._value, _value_schema_id=excluded._value_schema_id,
                        _value_message_index=excluded._value_message_index,
                        _headers=excluded._headers, _decode_error=excluded._decode_error
                    WHERE excluded._offset > %s._offset
                    """.formatted(table);
            try (PreparedStatement put = writer.prepareStatement(insert);
                 PreparedStatement delete = compacted ? writer.prepareStatement(
                         "DELETE FROM " + table + " WHERE _partition=? AND _key_raw=? AND _offset < ?") : null) {
                for (var record : batch.records()) {
                    if (!eligible.contains(record.offset())) continue;
                    var decoded = record.decoded();
                    if (compacted && record.tombstone()) {
                        delete.setInt(1, batch.partition()); delete.setBytes(2, record.key());
                        delete.setLong(3, record.offset()); delete.executeUpdate();
                    } else {
                        put.setInt(1, batch.partition()); put.setLong(2, record.offset()); put.setLong(3, record.timestamp());
                        put.setBytes(4, record.key()); put.setString(5, decoded.key().json()); put.setString(6, decoded.key().schemaRef());
                        put.setString(7, decoded.value().json()); put.setString(8, decoded.value().schemaRef());
                        put.setString(9, headers(record.headers())); put.setString(10, decoded.error());
                        put.setString(11, decoded.key().messageIndex()); put.setString(12, decoded.value().messageIndex());
                        // Compact updates/deletes must keep Kafka order, including repeated keys within this batch.
                        if (compacted) put.executeUpdate(); else put.addBatch();
                    }
                    schema(schemas, keyIds, decoded.key()); schema(schemas, valueIds, decoded.value());
                }
                if (!compacted) put.executeBatch();
            }
            for (var range : eligible.asRanges()) {
                RangeStore.add(writer, batch.generation(), batch.partition(), range.lowerEndpoint(), range.upperEndpoint());
            }
            for (var schema : schemas.values()) SchemaStore.store(writer, schema);
            TypedViews.refresh(writer, batch.generation(), keyIds, valueIds);
        }
        if (batch.reachedEnd() && batch.plan().checked()) {
            PartitionStore.caughtUp(writer, batch.generation(), batch.partition(), batch.plan().ends().capturedAtMs());
        }
    }

    private static void schema(Map<String, SchemaStore.StoredSchema> schemas, Set<SchemaStore.RecordType> ids, RecordDecoder.Part part) {
        if (part.schemaRef() != null) ids.add(new SchemaStore.RecordType(part.schemaRef(), part.messageIndex()));
        if (part.schema() != null) schemas.putIfAbsent(part.schema().ref(), part.schema());
    }

    /** Preserve header order/duplicates; strictly decoded UTF-8 is text, other bytes are base64. */
    private static String headers(Headers headers) throws SQLException {
        if (headers == null) return null;
        var iterator = headers.iterator();
        if (!iterator.hasNext()) return "[]";
        var utf8 = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
        StringWriter text = new StringWriter();
        try (JsonGenerator json = JacksonMapper.INSTANCE.getFactory().createGenerator(text)) {
            json.writeStartArray();
            do {
                var header = iterator.next();
                json.writeStartObject(); json.writeStringField("key", header.key());
                byte[] bytes = header.value();
                if (bytes == null) json.writeNullField("value");
                else {
                    try {
                        var value = utf8.decode(ByteBuffer.wrap(bytes));
                        json.writeFieldName("value");
                        json.writeString(value.array(), value.arrayOffset() + value.position(), value.remaining());
                    } catch (CharacterCodingException e) {
                        json.writeFieldName("base64"); json.writeBinary(bytes);
                    }
                }
                json.writeEndObject();
            } while (iterator.hasNext());
            json.writeEndArray();
        } catch (IOException e) { throw new SQLException("Encoding Kafka headers", e); }
        return text.toString();
    }
}
