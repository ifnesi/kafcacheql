package io.github.ifnesi.kafcacheql.schema;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.TypeRegistry;
import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
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

/** Single-threaded record decoding; no writes. */
public final class RecordDecoder implements AutoCloseable {
    @FunctionalInterface public interface RegistrySource { SchemaRegistry get() throws SQLException; }
    /** Schema copies travel with the decoded data; only the caller's later write transaction stores them. */
    public record Part(String json, String schemaRef, StoredSchema schema, String error, String messageIndex) {}
    public record Decoded(Part key, Part value, String error) {}

    private static final Part NULL = new Part(null, null, null, null, null);
    private static final JsonFormat.Printer PROTOBUF_JSON = JsonFormat.printer()
            .alwaysPrintFieldsWithNoPresence().preservingProtoFieldNames().omittingInsignificantWhitespace();
    // Confluent's JacksonMapper accepts non-JSON numbers such as NaN. Payload detection must be strict.
    private static final JsonFactory JSON = new JsonFactory();

    private final ClientSettings settings;
    private final Map<String, Object> properties;
    private final Connection reader;
    private final RegistrySource shared;
    private final CharsetDecoder utf8 = StandardCharsets.UTF_8.newDecoder();
    // The library adds the file's messages and transitive imports; descriptors are already resolved locally.
    private final LoadingCache<FileDescriptor, JsonFormat.Printer> protobufJson = CacheBuilder.newBuilder()
            .maximumSize(1000).build(CacheLoader.from(file -> PROTOBUF_JSON.usingTypeRegistry(
                    TypeRegistry.newBuilder().add(file.getMessageTypes()).build())));
    private SchemaRegistry registry;
    private SchemaRegistry.Client client;
    private KafkaAvroDeserializer keyAvro;
    private KafkaAvroDeserializer valueAvro;
    private KafkaJsonSchemaDeserializer<JsonNode> keyJson;
    private KafkaJsonSchemaDeserializer<JsonNode> valueJson;
    private KafkaProtobufDeserializer<Message> keyProtobuf;
    private KafkaProtobufDeserializer<Message> valueProtobuf;

    /** Reused across topics and records; not thread-safe. Reader is borrowed. */
    public RecordDecoder(ClientSettings settings, Connection reader) {
        this(settings, reader, null);
    }

    public RecordDecoder(ClientSettings settings, Connection reader, RegistrySource shared) {
        this.settings = settings;
        properties = settings.schemaRegistry();
        this.reader = reader;
        this.shared = shared;
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
        if (format == Format.AUTO || format == Format.AVRO || format == Format.JSON_SR || format == Format.PROTOBUF) {
            var reference = SchemaReferences.inspect(headers, payload, key);
            if (reference.malformed()) {
                return failed(payload, key, null, null, "Malformed schema header");
            }
            ref = reference.ref().orElse(null);
            if (ref != null) {
                if (!properties.containsKey("schema.registry.url") || properties.get("schema.registry.url").toString().isBlank()) {
                    return failed(payload, key, ref, null, "schema.registry.url is needed to decode schema " + ref);
                }
                if (registry == null) {
                    registry = shared == null ? SchemaRegistry.open(settings) : shared.get();
                    client = registry.decoderClient(reader);
                }
                var resolved = registry.resolve(reader, ref);
                if (resolved.isPresent()) {
                    stored = resolved.get().stored();
                    boolean avro = AvroSchema.TYPE.equals(stored.type()), json = JsonSchema.TYPE.equals(stored.type()),
                            protobuf = ProtobufSchema.TYPE.equals(stored.type());
                    if (!avro && !json && !protobuf) {
                        return failed(payload, key, ref, stored, "Unsupported schema type " + stored.type());
                    }
                    if (format == Format.AVRO && !avro || format == Format.JSON_SR && !json || format == Format.PROTOBUF && !protobuf) {
                        return failed(payload, key, ref, stored, "Schema type " + stored.type() + " does not match format " + format.value());
                    }
                    var schema = registry.parse(resolved.get()).parsed();
                    // Configuration failures fail the operation; corrupt record data is caught below.
                    var avroDecoder = avro ? avro(key) : null;
                    var jsonDecoder = json ? json(key) : null;
                    var protobufDecoder = protobuf ? protobuf(key) : null;
                    try {
                        String decoded, messageIndex = null;
                        if (avro) {
                            var raw = ((AvroSchema) schema).rawSchema();
                            decoded = AvroJsonConverter.convert(raw, avroDecoder.deserialize(topic, headers, payload, raw));
                        } else if (json) {
                            decoded = JacksonMapper.INSTANCE.writeValueAsString(jsonDecoder.deserialize(topic, headers, payload));
                        } else {
                            Message message = protobufDecoder.deserialize(topic, headers, payload);
                            decoded = protobufJson.getUnchecked(message.getDescriptorForType().getFile()).print(message);
                            messageIndex = JacksonMapper.INSTANCE.writeValueAsString(((ProtobufSchema) schema)
                                    .toMessageIndexes(message.getDescriptorForType().getFullName()).indexes());
                        }
                        return new Part(decoded, ref, stored, null, messageIndex);
                    } catch (RuntimeException | IOException e) {
                        String reason = io.github.ifnesi.kafcacheql.config.ConfigErrors.detail(e);
                        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                            if (cause instanceof java.io.EOFException) reason = "the value ends before the record does";
                            if (cause instanceof SchemaRegistry.LookupFailure lookup) {
                                throw lookup.failure();
                            }
                        }
                        String name = avro ? "Avro" : json ? "JSON Schema" : "Protobuf";
                        return failed(payload, key, ref, stored, name + ", schema " + ref + ": "
                                + reason);
                    }
                }
            }
            if (format == Format.AVRO || format == Format.JSON_SR || format == Format.PROTOBUF) {
                return failed(payload, key, ref, stored, ref == null ? "Missing " + format.value() + " schema reference" : "Unknown schema " + ref);
            }
        }
        if (format == Format.BYTES) {
            return new Part(json(payload), null, null, null, null);
        }
        String text;
        try {
            text = utf8.decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException e) {
            return format == Format.AUTO ? new Part(json(payload), null, null, null, null)
                    : failed(payload, key, null, null, "Invalid UTF-8");
        }
        if (format == Format.STRING) {
            return new Part(json(text), null, null, null, null);
        }
        if (validJson(text)) {
            return new Part(text, null, null, null, null);
        }
        return format == Format.AUTO ? new Part(json(text), null, null, null, null)
                : failed(payload, key, null, null, "Invalid JSON");
    }

    private KafkaAvroDeserializer avro(boolean key) throws SQLException {
        if (key) {
            if (keyAvro == null) {
                keyAvro = registry.avro(true, client);
            }
            return keyAvro;
        }
        if (valueAvro == null) {
            valueAvro = registry.avro(false, client);
        }
        return valueAvro;
    }

    private KafkaJsonSchemaDeserializer<JsonNode> json(boolean key) throws SQLException {
        if (key) {
            if (keyJson == null) keyJson = registry.json(true, client);
            return keyJson;
        }
        if (valueJson == null) valueJson = registry.json(false, client);
        return valueJson;
    }

    private Part failed(byte[] payload, boolean key, String ref, StoredSchema stored, String error) {
        return new Part(json(payload), ref, stored,
                Secrets.redact((key ? "key: " : "value: ") + error, properties), null);
    }

    private KafkaProtobufDeserializer<Message> protobuf(boolean key) throws SQLException {
        if (key) {
            if (keyProtobuf == null) keyProtobuf = registry.protobuf(true, client);
            return keyProtobuf;
        }
        if (valueProtobuf == null) valueProtobuf = registry.protobuf(false, client);
        return valueProtobuf;
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
            try (SchemaRegistry schemas = shared == null ? registry : null;
                 KafkaAvroDeserializer value = valueAvro;
                 KafkaAvroDeserializer key = keyAvro;
                 KafkaJsonSchemaDeserializer<JsonNode> jsonValue = valueJson;
                 KafkaJsonSchemaDeserializer<JsonNode> jsonKey = keyJson;
                 KafkaProtobufDeserializer<Message> protobufValue = valueProtobuf;
                 KafkaProtobufDeserializer<Message> protobufKey = keyProtobuf) {
                // Close the deserializers before their shared client, preserving suppressed failures.
            } catch (SQLException | RuntimeException e) {
                throw new SQLException(sentence(Secrets.redact("Closing decoder: " + io.github.ifnesi.kafcacheql.config.ConfigErrors.detail(e), properties)),
                        "08001", Secrets.sanitize(e, properties));
            }
            return null;
        });
    }
}
