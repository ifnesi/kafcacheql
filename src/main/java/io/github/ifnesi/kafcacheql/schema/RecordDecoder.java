package io.github.ifnesi.kafcacheql.schema;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.StoredSchema;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

/** Record decoding on the dedicated decoder thread; no writes or consumer ownership. */
public final class RecordDecoder implements AutoCloseable {
    /** Schema copies travel with the decoded data; only the caller's later write transaction stores them. */
    public record Part(String json, String schemaRef, StoredSchema schema, String error) {}
    public record Decoded(Part key, Part value, String error) {}

    private static final Part NULL = new Part(null, null, null, null);
    // Confluent's JacksonMapper accepts non-JSON numbers such as NaN. Payload detection must be strict.
    private static final JsonFactory JSON = new JsonFactory();

    private final ClientSettings settings;
    private final Map<String, Object> properties;
    private final Connection reader;
    private final CharsetDecoder utf8 = StandardCharsets.UTF_8.newDecoder();
    private SchemaRegistry registry;
    private KafkaAvroDeserializer keyAvro;
    private KafkaAvroDeserializer valueAvro;

    /** One decoder per worker, reused across topics and records; not thread-safe. Reader is borrowed. */
    public RecordDecoder(ClientSettings settings, Connection reader) {
        this.settings = settings;
        properties = settings.schemaRegistry();
        this.reader = reader;
    }

    public Decoded decode(String topic, Headers headers, byte[] key, byte[] value,
                          Format keyFormat, Format valueFormat) throws SQLException {
        return DriverLoader.call(() -> {
            Part decodedKey = part(topic, headers, key, true, keyFormat);
            Part decodedValue = part(topic, headers, value, false, valueFormat);
            String error = decodedKey.error() == null ? decodedValue.error() : decodedValue.error() == null
                    ? decodedKey.error() : decodedKey.error() + "; " + decodedValue.error();
            return new Decoded(decodedKey, decodedValue, error);
        });
    }

    private Part part(String topic, Headers headers, byte[] payload, boolean key, Format format) throws SQLException {
        if (payload == null) {
            return NULL;
        }
        String ref = null;
        StoredSchema stored = null;
        if (format == Format.AUTO || format == Format.AVRO) {
            var reference = SchemaReferences.inspect(headers, payload, key);
            if (reference.malformed()) {
                return failed(payload, key, null, null, "Malformed schema header");
            }
            ref = reference.ref().orElse(null);
            if (ref != null) {
                if (registry == null) {
                    registry = SchemaRegistry.open(settings, reader);
                }
                var resolved = registry.resolve(reader, ref);
                if (resolved.isPresent()) {
                    stored = resolved.get().stored();
                    if (!AvroSchema.TYPE.equals(stored.type())) {
                        return failed(payload, key, ref, stored, "Schema type " + stored.type() + " requires phase 2");
                    }
                    AvroSchema schema = (AvroSchema) registry.parse(resolved.get()).parsed();
                    // Configuration failures fail the operation; corrupt record data is caught below.
                    KafkaAvroDeserializer deserializer = avro(key);
                    try {
                        Object datum = deserializer.deserialize(topic, headers, payload, schema.rawSchema());
                        return new Part(AvroJsonConverter.convert(schema.rawSchema(), datum), ref, stored, null);
                    } catch (RuntimeException | IOException e) {
                        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                            if (cause instanceof SchemaRegistry.LookupFailure lookup) {
                                throw lookup.failure();
                            }
                        }
                        return failed(payload, key, ref, stored, e.getMessage());
                    }
                }
            }
            if (format == Format.AVRO) {
                return failed(payload, key, ref, stored, ref == null ? "Missing Avro schema reference" : "Unknown schema " + ref);
            }
        }
        if (format == Format.BYTES) {
            return new Part(json(payload), null, null, null);
        }
        if (format == Format.JSON_SR || format == Format.PROTOBUF) {
            return failed(payload, key, ref, stored, "Format " + format.value() + " requires phase 2");
        }
        String text;
        try {
            text = utf8.decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException e) {
            return format == Format.AUTO ? new Part(json(payload), null, null, null)
                    : failed(payload, key, null, null, "Invalid UTF-8");
        }
        if (format == Format.STRING) {
            return new Part(json(text), null, null, null);
        }
        if (validJson(text)) {
            return new Part(text, null, null, null);
        }
        return format == Format.AUTO ? new Part(json(text), null, null, null)
                : failed(payload, key, null, null, "Invalid JSON");
    }

    private KafkaAvroDeserializer avro(boolean key) throws SQLException {
        if (key) {
            if (keyAvro == null) {
                keyAvro = registry.avro(true);
            }
            return keyAvro;
        }
        if (valueAvro == null) {
            valueAvro = registry.avro(false);
        }
        return valueAvro;
    }

    private Part failed(byte[] payload, boolean key, String ref, StoredSchema stored, String error) {
        return new Part(json(payload), ref, stored,
                Secrets.redact((key ? "key: " : "value: ") + error, properties));
    }

    private static boolean validJson(String text) {
        try (JsonParser parser = JSON.createParser(text)) {
            if (parser.nextToken() == null) {
                return false;
            }
            parser.skipChildren();
            return parser.nextToken() == null;
        } catch (IOException invalid) {
            return false;
        }
    }

    private static String json(Object value) {
        try {
            return JacksonMapper.INSTANCE.writeValueAsString(value);
        } catch (IOException impossible) {
            throw new AssertionError("Jackson cannot encode a string or bytes", impossible);
        }
    }

    @Override
    @SuppressWarnings("try") // These resources exist for their close actions and suppression order.
    public void close() throws SQLException {
        DriverLoader.call(() -> {
            try (SchemaRegistry schemas = registry;
                 KafkaAvroDeserializer value = valueAvro;
                 KafkaAvroDeserializer key = keyAvro) {
                // Close both deserializers before their shared client, preserving suppressed failures.
            } catch (SQLException | RuntimeException e) {
                throw new SQLException(Secrets.redact("Closing decoder: " + e.getMessage(), properties),
                        "08001", Secrets.sanitize(e, properties));
            }
            return null;
        });
    }
}
