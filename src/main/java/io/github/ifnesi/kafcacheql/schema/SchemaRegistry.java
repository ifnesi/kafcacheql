package io.github.ifnesi.kafcacheql.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.github.ifnesi.kafcacheql.cache.SchemaStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.StoredSchema;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.SubjectSchema;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Registry reads outside the write lock; returned values are persisted by the caller. */
public final class SchemaRegistry implements AutoCloseable {
    private final Map<String, Object> settings;
    private final String urls;
    private final Client client;
    private final Transport transport;
    private final Cache<String, ResolvedSchema> schemas;

    private static final TypeReference<List<SchemaReference>> REFERENCES = new TypeReference<>() {};

    record ResolvedSchema(StoredSchema stored, ParsedSchema parsed) {}

    private SchemaRegistry(Map<String, Object> settings, AbstractKafkaSchemaSerDeConfig config, Connection reader) {
        this.settings = settings;
        urls = String.join(",", config.getSchemaRegistryUrls());
        schemas = CacheBuilder.newBuilder().maximumSize(config.getMaxSchemasPerSubject()).build();
        transport = new Transport(config.getSchemaRegistryUrls());
        client = new Client(config, reader);
    }

    public static SchemaRegistry open(ClientSettings settings) throws SQLException {
        return open(settings, null);
    }

    /** The reader is borrowed, never closed here. The decoder thread uses it exclusively. */
    static SchemaRegistry open(ClientSettings settings, Connection reader) throws SQLException {
        Map<String, Object> registry = settings.schemaRegistry();
        return DriverLoader.call(() -> {
            try {
                var config = new AbstractKafkaSchemaSerDeConfig(
                        AbstractKafkaSchemaSerDeConfig.baseConfigDef(), registry, false);
                return new SchemaRegistry(registry, config, reader);
            } catch (RuntimeException e) {
                throw failure("opening client", String.valueOf(registry.get("schema.registry.url")), e, registry);
            }
        });
    }

    public Optional<StoredSchema> lookup(Connection reader, String ref) throws SQLException {
        return resolve(reader, ref).map(ResolvedSchema::stored);
    }

    Optional<ResolvedSchema> resolve(Connection reader, String ref) throws SQLException {
        return DriverLoader.call(() -> {
            ResolvedSchema cached = schemas.getIfPresent(ref);
            if (cached != null) {
                return Optional.of(cached);
            }
            try {
                Optional<StoredSchema> stored = reader == null ? Optional.empty() : SchemaStore.find(reader, ref);
                ResolvedSchema resolved;
                if (stored.isPresent()) {
                    // Merely looking up a stored schema must not resolve its references over the network.
                    resolved = new ResolvedSchema(stored.get(), null);
                } else {
                    resolved = new ResolvedSchema(client.fetch(ref), null);
                }
                schemas.put(ref, resolved);
                return Optional.of(resolved);
            } catch (LookupFailure e) {
                throw e.failure();
            } catch (RestClientException e) {
                if (e.getErrorCode() == 40403) {
                    return Optional.empty();
                }
                throw failure("looking up schema " + ref, urls, e, settings);
            } catch (IOException | RuntimeException e) {
                throw failure("looking up schema " + ref, urls, e, settings);
            }
        });
    }

    /** Parse a durable schema only when decoding needs it; Confluent resolves its references. */
    ResolvedSchema parse(ResolvedSchema schema) throws SQLException {
        if (schema.parsed() != null) {
            return schema;
        }
        return DriverLoader.call(() -> {
            StoredSchema row = schema.stored();
            try {
                List<SchemaReference> refs = row.refs() == null ? List.of()
                        : JacksonMapper.INSTANCE.readValue(row.refs(), REFERENCES);
                ParsedSchema parsed = client.parseSchema(row.type(), row.schemaText(), refs)
                        .orElseThrow(() -> new IOException("Cannot parse cached schema " + row.ref()));
                ResolvedSchema resolved = new ResolvedSchema(row, parsed);
                schemas.put(row.ref(), resolved);
                return resolved;
            } catch (LookupFailure e) {
                throw e.failure();
            } catch (IOException | RuntimeException e) {
                throw failure("parsing schema " + row.ref(), urls, e, settings);
            }
        });
    }

    KafkaAvroDeserializer avro(boolean key) throws SQLException {
        return DriverLoader.call(() -> {
            try {
                return new KafkaAvroDeserializer(client, settings, key);
            } catch (RuntimeException e) {
                throw failure("configuring Avro decoder", urls, e, settings);
            }
        });
    }

    public Optional<SubjectSchema> latest(String topic, boolean key) throws SQLException {
        return DriverLoader.call(() -> {
            String subject = new TopicNameStrategy().subjectName(topic, key, null);
            try {
                // -1 is the registry's latest-version selector. This client API fetches live metadata;
                // getLatestSchemaMetadata caches it indefinitely by default, contrary to PLAN §7.3.
                SchemaMetadata schema = client.getSchemaMetadata(subject, -1);
                return Optional.of(new SubjectSchema(subject, schema.getVersion(), new StoredSchema(
                        Integer.toString(schema.getId()), schema.getSchemaType(), schema.getSchema(),
                        JacksonMapper.INSTANCE.writeValueAsString(schema.getReferences()), System.currentTimeMillis())));
            } catch (LookupFailure e) {
                throw e.failure();
            } catch (RestClientException e) {
                if (e.getErrorCode() == 40401) {
                    return Optional.empty();
                }
                throw failure("refreshing subject " + subject, urls, e, settings);
            } catch (IOException | RuntimeException e) {
                throw failure("refreshing subject " + subject, urls, e, settings);
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
        return new SQLException(Secrets.redact("Schema Registry at " + urls + ": " + action
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
    private final class Client extends CachedSchemaRegistryClient {
        private final Connection reader;

        Client(AbstractKafkaSchemaSerDeConfig config, Connection reader) {
            super(transport, config.getMaxSchemasPerSubject(), settings, config.requestHeaders());
            this.reader = reader;
        }

        StoredSchema fetch(String ref) throws IOException, RestClientException {
            // Resolve metadata before choosing a parser: phase-2 schema types are still cacheable.
            var schema = ref.indexOf('-') >= 0 ? transport.getByGuid(ref, null)
                    : transport.getId(Integer.parseInt(ref));
            return new StoredSchema(ref, java.util.Objects.requireNonNullElse(schema.getSchemaType(), AvroSchema.TYPE),
                    schema.getSchemaString(), JacksonMapper.INSTANCE.writeValueAsString(schema.getReferences()),
                    System.currentTimeMillis());
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
     * is fatal, including lookups Confluent performs for schema references.
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
