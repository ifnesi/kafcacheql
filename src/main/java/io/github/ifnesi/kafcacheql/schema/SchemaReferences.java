package io.github.ifnesi.kafcacheql.schema;

import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** Schema identity only; payload decoding belongs to the Confluent deserializers. */
public final class SchemaReferences {
    private SchemaReferences() {}

    public static Optional<String> find(Headers headers, byte[] payload, boolean key) {
        return inspect(headers, payload, key).ref();
    }

    /** A malformed present header must be distinguished from a record with no schema reference. */
    public record Reference(Optional<String> ref, boolean malformed) {}

    public static Reference inspect(Headers headers, byte[] payload, boolean key) {
        if (payload == null) {
            return new Reference(Optional.empty(), false);
        }
        Header header = headers == null ? null
                : headers.lastHeader(key ? SchemaId.KEY_SCHEMA_ID_HEADER : SchemaId.VALUE_SCHEMA_ID_HEADER);
        byte[] bytes = header == null ? null : header.value();
        if (bytes == null) {
            if (payload.length < 1 + SchemaId.ID_SIZE || payload[0] != SchemaId.MAGIC_BYTE_V0) {
                return new Reference(Optional.empty(), false);
            }
            bytes = payload;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return DriverLoader.call(() -> {
            // Only identity is needed here. AVRO tells SchemaId not to consume Protobuf message indexes;
            // the eventual deserializer handles those, after lookup establishes the schema type.
            SchemaId id = new SchemaId("AVRO");
            try {
                id.fromBytes(buffer);
                return new Reference(Optional.of(id.getGuid() == null ? id.getId().toString() : id.getGuid().toString()), false);
            } catch (BufferUnderflowException | IllegalArgumentException malformed) {
                // A present, malformed header isn't replaced by a prefix: Confluent checks it first too.
                return new Reference(Optional.empty(), true);
            }
        });
    }
}
