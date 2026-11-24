package io.github.ifnesi.kafcacheql.testkit;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * The Docker Compose test stack (docker-compose.yml) and helpers for
 * integration tests. Every test uses its own topics ({@link #uniqueTopic}), so the stack can stay
 * up between runs.
 *
 * <p>Timestamps: the stack checks retention every second, so a record whose timestamp is older than the
 * topic's retention.ms (7 days by default) is deleted within a second or so of being written. Tests that
 * choose timestamps use recent ones, such as {@link #recent}, unless they test retention.
 */
public final class TestStack {

    public static final String BOOTSTRAP = System.getProperty("test.kafka.bootstrap", "localhost:9092");
    public static final String REGISTRY = System.getProperty("test.schema.registry.url", "http://localhost:8081");

    /** Where an Avro record's schema reference goes. */
    public enum SchemaIdIn { PREFIX, HEADER }

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String HEADER_ID_SERIALIZER =
            "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer";

    private static Admin admin;
    private static KafkaProducer<byte[], byte[]> producer;
    private static final Map<String, KafkaAvroSerializer> AVRO = new HashMap<>();

    private TestStack() {}

    // ------------------------------------------------------------------ the stack

    /** Fails at once, saying how to start the stack, when the broker or the registry isn't up. */
    public static synchronized void requireUp() {
        String hint = " isn't reachable. Start the test stack: docker compose up -d broker schema-registry";
        try {
            admin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka at " + BOOTSTRAP + hint, e);
        }
        try {
            HttpResponse<Void> r = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(REGISTRY + "/subjects")).timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.discarding());
            if (r.statusCode() != 200) {
                throw new IllegalStateException("Schema Registry at " + REGISTRY + " answered " + r.statusCode());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Schema Registry at " + REGISTRY + hint, e);
        }
    }

    public static synchronized Admin admin() {
        if (admin == null) {
            admin = Admin.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                    AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000));
            Runtime.getRuntime().addShutdownHook(new Thread(TestStack::closeAll));
        }
        return admin;
    }

    private static synchronized void closeAll() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        AVRO.values().forEach(KafkaAvroSerializer::close);
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
    }

    // ------------------------------------------------------------------ topics

    /** A timestamp a minute ago plus {@code millis}: recent enough that retention keeps the record. */
    public static long recent(long millis) {
        return BASE_TIME + millis;
    }

    private static final long BASE_TIME = System.currentTimeMillis() - 60_000L;

    /** A topic name no other test or run uses: {@code base-xxxxxxxx}. */
    public static String uniqueTopic(String base) {
        return base + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Creates a topic (replication factor 1) and waits until it is visible. */
    public static void createTopic(String name, int partitions, Map<String, String> configs) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (true) {
            try {
                get(admin().createTopics(List.of(new NewTopic(name, partitions, (short) 1).configs(configs))).all());
                break;
            } catch (TopicExistsException e) {
                // A deletion of the same name is still in progress (recreateTopic).
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                pause();
            }
        }
        await(() -> topicExists(name), "topic " + name + " visible");
    }

    public static void deleteTopic(String name) {
        get(admin().deleteTopics(List.of(name)).all());
        await(() -> !topicExists(name), "topic " + name + " deleted");
    }

    /** Deletes the topic and creates it again: same name, new topic ID. */
    public static void recreateTopic(String name, int partitions, Map<String, String> configs) {
        deleteTopic(name);
        createTopic(name, partitions, configs);
    }

    public static void addPartitions(String name, int totalPartitions) {
        get(admin().createPartitions(Map.of(name, NewPartitions.increaseTo(totalPartitions))).all());
        await(() -> partitionCount(name) == totalPartitions, "topic " + name + " has " + totalPartitions + " partitions");
    }

    public static boolean topicExists(String name) {
        return get(admin().listTopics().names()).contains(name);
    }

    public static TopicDescription describe(String name) {
        return get(admin().describeTopics(List.of(name)).allTopicNames()).get(name);
    }

    public static Uuid topicId(String name) {
        return describe(name).topicId();
    }

    public static int partitionCount(String name) {
        return describe(name).partitions().size();
    }

    /** The topic's configs that were set explicitly (not broker defaults). */
    public static Map<String, String> topicConfigs(String name) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = get(admin().describeConfigs(List.of(resource)).all()).get(resource);
        Map<String, String> explicit = new HashMap<>();
        for (ConfigEntry e : config.entries()) {
            if (e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                explicit.put(e.name(), e.value());
            }
        }
        return explicit;
    }

    // ------------------------------------------------------------------ producing

    /** Sends one record as raw bytes; {@code partition} and {@code timestamp} may be null. */
    public static RecordMetadata send(String topic, Integer partition, Long timestamp,
                                      byte[] key, byte[] value, Iterable<Header> headers) {
        return get(producer().send(new ProducerRecord<>(topic, partition, timestamp, key, value, headers)));
    }

    public static RecordMetadata sendString(String topic, String key, String value, Long timestamp) {
        return send(topic, null, timestamp, utf8(key), utf8(value), null);
    }

    public static RecordMetadata sendJson(String topic, String key, String json, Long timestamp) {
        return sendString(topic, key, json, timestamp);
    }

    public static RecordMetadata sendBytes(String topic, byte[] key, byte[] value, Long timestamp) {
        return send(topic, null, timestamp, key, value, null);
    }

    /** A null value for the key: a deletion on compacted topics. */
    public static RecordMetadata sendTombstone(String topic, String key, Long timestamp) {
        return send(topic, null, timestamp, utf8(key), null, null);
    }

    /**
     * An Avro value (a GenericRecord, or any Avro-supported object) with a plain string key. The schema
     * is registered under {@code <topic>-value}; its reference goes in the 5-byte prefix or in the
     * {@code __value_schema_id} header.
     */
    public static RecordMetadata sendAvro(String topic, String key, Object value, Long timestamp, SchemaIdIn where) {
        Headers headers = new RecordHeaders();
        byte[] bytes = avro(where, false).serialize(topic, headers, value);
        return send(topic, null, timestamp, utf8(key), bytes, headers);
    }

    /** Avro key and value, both registered (<topic>-key, <topic>-value) and referenced the same way. */
    public static RecordMetadata sendAvroKeyed(String topic, Object key, Object value, Long timestamp, SchemaIdIn where) {
        Headers headers = new RecordHeaders();
        byte[] k = avro(where, true).serialize(topic, headers, key);
        byte[] v = avro(where, false).serialize(topic, headers, value);
        return send(topic, null, timestamp, k, v, headers);
    }

    /**
     * Runs {@code body} in a transaction of a new transactional producer writing to {@code topic}, then
     * commits or aborts it. The records are flushed first, so an aborted transaction's records are in the
     * log too. Returns once the broker has written the transaction's markers: until then the last stable
     * offset still holds read_committed readers back.
     */
    public static void inTransaction(String topic, boolean commit, Consumer<Producer<byte[], byte[]>> body) {
        Map<String, Object> props = producerProps();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafcacheql-test-" + UUID.randomUUID());
        try (KafkaProducer<byte[], byte[]> tx = new KafkaProducer<>(props)) {
            tx.initTransactions();
            tx.beginTransaction();
            body.accept(tx);
            tx.flush();
            if (commit) {
                tx.commitTransaction();
            } else {
                tx.abortTransaction();
            }
        }
        awaitTransactionsSettled(topic);
    }

    /** Waits until no transaction holds the topic's last stable offset below its high watermark. */
    public static void awaitTransactionsSettled(String topic) {
        await(() -> endOffsets(topic, IsolationLevel.READ_COMMITTED).equals(endOffsets(topic, IsolationLevel.READ_UNCOMMITTED)),
                "the transaction markers of " + topic);
    }

    private static Map<Integer, Long> endOffsets(String topic, IsolationLevel isolation) {
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        for (int p = 0; p < partitionCount(topic); p++) {
            query.put(new TopicPartition(topic, p), OffsetSpec.latest());
        }
        Map<Integer, Long> out = new HashMap<>();
        get(admin().listOffsets(query, new ListOffsetsOptions(isolation)).all())
                .forEach((tp, info) -> out.put(tp.partition(), info.offset()));
        return out;
    }

    /** A new transactional producer, transactions initialised; the caller begins, ends and closes. */
    public static KafkaProducer<byte[], byte[]> transactionalProducer() {
        Map<String, Object> props = producerProps();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafcacheql-test-" + UUID.randomUUID());
        KafkaProducer<byte[], byte[]> tx = new KafkaProducer<>(props);
        tx.initTransactions();
        return tx;
    }

    private static synchronized KafkaProducer<byte[], byte[]> producer() {
        if (producer == null) {
            producer = new KafkaProducer<>(producerProps());
        }
        return producer;
    }

    private static Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        return props;
    }

    private static synchronized KafkaAvroSerializer avro(SchemaIdIn where, boolean isKey) {
        return AVRO.computeIfAbsent(where + "/" + isKey, k -> {
            Map<String, Object> config = new HashMap<>();
            config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY);
            if (where == SchemaIdIn.HEADER) {
                config.put(isKey ? "key.schema.id.serializer" : "value.schema.id.serializer", HEADER_ID_SERIALIZER);
            }
            KafkaAvroSerializer serializer = new KafkaAvroSerializer();
            serializer.configure(config, isKey);
            return serializer;
        });
    }

    // ------------------------------------------------------------------ reading

    /**
     * Every record of the topic, from each partition's start to its end offset as of the call, read with
     * {@code read_committed} or {@code read_uncommitted}. Records appended after the end offsets were taken
     * aren't returned.
     */
    public static List<ConsumerRecord<byte[], byte[]>> readAll(String topic, boolean readCommitted) {
        return readAll(topic, readCommitted, () -> {});
    }

    /** As {@link #readAll(String, boolean)}, running {@code afterSnapshot} once the end offsets are taken. */
    public static List<ConsumerRecord<byte[], byte[]>> readAll(String topic, boolean readCommitted, Runnable afterSnapshot) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic, TIMEOUT).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions, TIMEOUT);
            afterSnapshot.run();
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            // Done by position, not by count: transaction markers and aborted records take offsets too.
            while (partitions.stream().anyMatch(tp -> consumer.position(tp, TIMEOUT) < end.get(tp))) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("timed out reading " + topic);
                }
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                    if (r.offset() < end.get(new TopicPartition(r.topic(), r.partition()))) {
                        out.add(r);
                    }
                }
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ small things

    public static byte[] utf8(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    public static String utf8(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    /** Waits up to 30 s for {@code condition}. */
    public static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for " + what);
            }
            pause();
        }
    }

    private static void pause() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static <T> T get(java.util.concurrent.Future<T> future) {
        try {
            return future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException r) {
                throw r;
            }
            throw new IllegalStateException(e.getCause());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
