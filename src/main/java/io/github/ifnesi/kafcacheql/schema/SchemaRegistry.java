package io.github.ifnesi.kafcacheql.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.github.ifnesi.kafcacheql.cache.SchemaStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.StoredSchema;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/** Registry reads outside the write lock; returned values are persisted by the caller. */
public final class SchemaRegistry implements AutoCloseable {
    private final Map<String, Object> settings;
    private final String urls;
    private final Client client;
    private final AbstractKafkaSchemaSerDeConfig config;
    private final Transport transport;
    private final Cache<String, ResolvedSchema> schemas;
    private final Cache<Reference, io.confluent.kafka.schemaregistry.client.rest.entities.Schema> references;

    private record Reference(String subject, int version, boolean deleted) {}
    record ResolvedSchema(StoredSchema stored, ParsedSchema parsed) {}
    private record ResolvedReferences(List<SchemaReference> references, Map<String, String> resolved) {}

    private SchemaRegistry(Map<String, Object> settings, AbstractKafkaSchemaSerDeConfig config) {
        this.settings = settings;
        this.config = config;
        urls = String.join(",", config.getSchemaRegistryUrls());
        schemas = CacheBuilder.newBuilder().maximumSize(config.getMaxSchemasPerSubject()).build();
        references = CacheBuilder.newBuilder().maximumSize(config.getMaxSchemasPerSubject()).build();
        transport = new Transport(config.getSchemaRegistryUrls());
        client = new Client(null, true);
    }

    public static SchemaRegistry open(ClientSettings settings) throws SQLException {
        Map<String, Object> registry = settings.schemaRegistry();
        return DriverLoader.call(() -> {
            try {
                var config = new AbstractKafkaSchemaSerDeConfig(
                        AbstractKafkaSchemaSerDeConfig.baseConfigDef(), registry, false);
                return new SchemaRegistry(registry, config);
            } catch (RuntimeException e) {
                throw failure("opening client", java.util.Objects.toString(registry.get("schema.registry.url"), ""), e, registry);
            }
        });
    }

    public Optional<StoredSchema> lookup(Connection reader, String ref) throws SQLException {
        return resolve(reader, ref).map(ResolvedSchema::stored);
    }

    Optional<ResolvedSchema> resolve(Connection reader, String ref) throws SQLException {
        return DriverLoader.call(() -> {
            try {
                return Optional.of(schemas.get(ref, () -> {
                    Optional<StoredSchema> stored = reader == null ? Optional.empty() : SchemaStore.find(reader, ref);
                    // Looking up a stored schema must not resolve its references over the network.
                    return new ResolvedSchema(stored.isPresent() ? stored.get() : client.fetch(ref), null);
                }));
            } catch (ExecutionException | UncheckedExecutionException e) {
                var cause = e.getCause();
                if (cause instanceof SQLException sql) throw sql;
                if (cause instanceof LookupFailure lookup) throw lookup.failure();
                if (cause instanceof RestClientException rest && rest.getErrorCode() == 40403) return Optional.empty();
                throw failure("looking up schema " + ref, urls, (Exception) cause, settings);
            }
        });
    }

    /** Parse a schema and its already resolved references, without network access. */
    ResolvedSchema parse(ResolvedSchema schema) throws SQLException {
        if (schema.parsed() != null) {
            return schema;
        }
        return DriverLoader.call(() -> {
            StoredSchema row = schema.stored();
            try {
                ParsedSchema parsed = switch (row.type()) {
                    case JsonSchema.TYPE -> parseJson(row);
                    case ProtobufSchema.TYPE -> parseProtobuf(row);
                    default -> parseAvro(row);
                };
                ResolvedSchema resolved = new ResolvedSchema(row, parsed);
                schemas.put(row.ref(), resolved);
                return resolved;
            } catch (LookupFailure e) {
                throw e.failure();
            } catch (RuntimeException e) {
                throw failure("parsing schema " + row.ref(), urls, e, settings);
            }
        });
    }

    /** Shared offline parser for decoding and typed views. ID/GUID lookups request resolved Avro. */
    public static AvroSchema parseAvro(StoredSchema schema) {
        return new AvroSchema(schema.schemaText());
    }

    public static JsonSchema parseJson(StoredSchema schema) {
        try {
            var refs = JacksonMapper.INSTANCE.readValue(schema.refs(), ResolvedReferences.class);
            // Block remote URL resolution: every external body must come from the fixed references.
            return new JsonSchema(schema.schemaText(), refs.references(), refs.resolved(), null, null, null, true);
        } catch (IOException e) {
            throw new IllegalArgumentException("Reading cached JSON Schema references", e);
        }
    }

    public static ProtobufSchema parseProtobuf(StoredSchema schema) {
        try {
            var refs = JacksonMapper.INSTANCE.readValue(schema.refs(), ResolvedReferences.class);
            return new ProtobufSchema(schema.schemaText(), refs.references(), refs.resolved(), null, null);
        } catch (IOException e) {
            throw new IllegalArgumentException("Reading cached Protobuf references", e);
        }
    }

    /** Deserializers share caches and transport, but only their owner uses this borrowed reader. */
    Client decoderClient(Connection reader) { return new Client(reader, false); }

    KafkaAvroDeserializer avro(boolean key, Client client) throws SQLException {
        return DriverLoader.call(() -> {
            try {
                var config = new HashMap<>(settings);
                config.put(key ? "key.subject.name.strategy" : "value.subject.name.strategy",
                        io.confluent.kafka.serializers.subject.TopicNameStrategy.class);
                config.remove(AbstractKafkaSchemaSerDeConfig.USE_LATEST_WITH_METADATA);
                return new KafkaAvroDeserializer(client, config, key);
            } catch (RuntimeException e) {
                throw failure("configuring Avro decoder", urls, e, settings);
            }
        });
    }

    KafkaJsonSchemaDeserializer<JsonNode> json(boolean key, Client client) throws SQLException {
        return DriverLoader.call(() -> {
            try {
                var config = new HashMap<>(settings);
                config.put(key ? "json.key.type" : "json.value.type", JsonNode.class);
                // Subject associations and reader-schema selection are irrelevant to record IDs.
                config.put(key ? "key.subject.name.strategy" : "value.subject.name.strategy",
                        io.confluent.kafka.serializers.subject.TopicNameStrategy.class);
                config.remove(AbstractKafkaSchemaSerDeConfig.USE_LATEST_WITH_METADATA);
                var decoder = new KafkaJsonSchemaDeserializer<JsonNode>(client);
                decoder.configure(config, key);
                return decoder;
            } catch (RuntimeException e) {
                throw failure("configuring JSON Schema decoder", urls, e, settings);
            }
        });
    }

    KafkaProtobufDeserializer<Message> protobuf(boolean key, Client client) throws SQLException {
        return DriverLoader.call(() -> {
            try {
                var config = new HashMap<>(settings);
                config.put(key ? "key.subject.name.strategy" : "value.subject.name.strategy",
                        io.confluent.kafka.serializers.subject.TopicNameStrategy.class);
                config.remove(AbstractKafkaSchemaSerDeConfig.USE_LATEST_WITH_METADATA);
                config.put("derive.type", false);
                config.remove(key ? "specific.protobuf.key.type" : "specific.protobuf.value.type");
                var decoder = new KafkaProtobufDeserializer<Message>(client);
                decoder.configure(config, key);
                return decoder;
            } catch (RuntimeException e) {
                throw failure("configuring Protobuf decoder", urls, e, settings);
            }
        });
    }

    @Override
    public void close() throws SQLException {
        DriverLoader.call(() -> {
            try {
                client.closeOwned();
            } catch (IOException | RuntimeException e) {
                throw failure("closing client", urls, e, settings);
            }
            return null;
        });
    }

    private static SQLException failure(String action, String urls, Exception cause, Map<String, ?> settings) {
        return new SQLException(Secrets.redact("Schema Registry" + (urls.isBlank() ? "" : " at " + urls) + ": " + action
                + ": " + cause.getMessage(), settings), "08001", Secrets.sanitize(cause, settings));
    }

    /** Carries a registry/SQLite failure through Confluent's runtime-only deserializer API. */
    static final class LookupFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final SQLException failure;

        LookupFailure(SQLException failure) {
            super(failure);
            this.failure = failure;
        }

        SQLException failure() {
            return failure;
        }
    }

    /** Both auto detection and Confluent use the same cache-first resolution and parsed schema. */
    final class Client extends CachedSchemaRegistryClient {
        private final Connection reader;

        Client(Connection reader, boolean configureTransport) {
            // Configure the shared transport only once, before any decoder starts using it.
            super(transport, config.getMaxSchemasPerSubject(), configureTransport ? settings : Map.of(),
                    configureTransport ? config.requestHeaders() : null);
            this.reader = reader;
        }

        StoredSchema fetch(String ref) throws IOException, RestClientException {
            // Avro's endpoint resolves references; the other formats use Confluent's fixed-version resolver.
            var schema = ref.indexOf('-') >= 0 ? transport.getByGuid(ref, "resolved")
                    : transport.getId(RestService.DEFAULT_REQUEST_PROPERTIES, Integer.parseInt(ref), null, "resolved", null, false);
            String refs = JacksonMapper.INSTANCE.writeValueAsString(schema.getReferences());
            if (JsonSchema.TYPE.equals(schema.getSchemaType()) || ProtobufSchema.TYPE.equals(schema.getSchemaType())) {
                refs = JacksonMapper.INSTANCE.writeValueAsString(new ResolvedReferences(schema.getReferences(),
                        AbstractSchemaProvider.resolveReferences(this, null, schema.getReferences(), false, true)));
            }
            return new StoredSchema(ref, java.util.Objects.requireNonNullElse(schema.getSchemaType(), AvroSchema.TYPE),
                    schema.getSchemaString(), refs,
                    System.currentTimeMillis());
        }

        @Override
        public io.confluent.kafka.schemaregistry.client.rest.entities.Schema getByVersion(String subject, int version, boolean deleted) {
            if (version < 1) throw new IllegalArgumentException("Schema references must name a fixed positive version");
            try {
                return references.get(new Reference(subject, version, deleted), () -> super.getByVersion(subject, version, deleted));
            } catch (ExecutionException | UncheckedExecutionException e) {
                if (e.getCause() instanceof LookupFailure lookup) throw lookup;
                throw new LookupFailure(failure("looking up reference " + subject + " version " + version,
                        urls, (Exception) e.getCause(), settings));
            }
        }

        private ParsedSchema required(String ref) throws RestClientException {
            try {
                return parse(resolve(reader, ref)
                        .orElseThrow(() -> new RestClientException("Unknown schema " + ref, 404, 40403))).parsed();
            } catch (SQLException e) {
                throw new LookupFailure(e);
            }
        }

        @Override
        public ParsedSchema getSchemaBySubjectAndId(String subject, int id) throws RestClientException {
            return required(Integer.toString(id));
        }

        @Override
        public ParsedSchema getSchemaByGuid(String guid, String format) throws RestClientException {
            return required(guid);
        }

        @Override
        public void close() {
            // Shared by key and value deserializers; SchemaRegistry owns the client.
        }

        void closeOwned() throws IOException {
            super.close();
        }
    }

    /** Mark failures at the registry transport, so an Avro EOF is a row error while a network failure
     * is fatal, on the ID/GUID lookup endpoints.
     */
    private final class Transport extends RestService {
        Transport(List<String> urls) {
            super(urls);
        }

        @Override
        public <T> T httpRequest(String path, String method, byte[] body, Map<String, String> headers,
                                 TypeReference<T> responseType) throws IOException, RestClientException {
            try {
                return super.httpRequest(path, method, body, headers, responseType);
            } catch (RestClientException e) {
                if (e.getErrorCode() == 40401 || e.getErrorCode() == 40402 || e.getErrorCode() == 40403) {
                    throw e;
                }
                throw new LookupFailure(failure("requesting " + path, urls, e, settings));
            } catch (IOException | RuntimeException e) {
                throw new LookupFailure(failure("requesting " + path, urls, e, settings));
            }
        }
    }
}
