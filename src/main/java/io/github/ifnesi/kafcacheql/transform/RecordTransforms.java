package io.github.ifnesi.kafcacheql.transform;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import io.github.ifnesi.kafcacheql.config.TransformSettings;
import io.github.ifnesi.kafcacheql.schema.RecordDecoder;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

/** One immutable policy per topic and one hook instance per class per cache. No raw payload reaches storage. */
public final class RecordTransforms {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_LONG_FOR_INTS)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    public record Fingerprint(TransformSettings settings, String version, String secretId) {}
    private final Map<String, TopicTransform> topics = new HashMap<>();
    private final DriverConfig config;

    public RecordTransforms(DriverConfig config) throws SQLException {
        this.config = config;
        Map<String, RecordTransform> classes = new HashMap<>();
        for (var entry : config.topics().entrySet()) {
            TransformSettings settings = entry.getValue().transforms();
            if (!settings.enabled()) continue;
            RecordTransform hook = null;
            if (!settings.className().isEmpty()) {
                hook = classes.get(settings.className());
                if (hook == null) {
                    try {
                        hook = Class.forName(settings.className(), true, DriverLoader.LOADER)
                                .asSubclass(RecordTransform.class).getConstructor().newInstance();
                        classes.put(settings.className(), hook);
                    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
                        throw ConfigErrors.problem(Secrets.redact("Loading record transform " + settings.className() + ": " + ConfigErrors.detail(e),
                                config.redactions()), Secrets.sanitize(e, config.redactions()));
                    }
                }
            }
            try { topics.put(entry.getKey(), new TopicTransform(settings, hook, config.maskSecret().orElseThrow())); }
            catch (VirtualMachineError e) { throw e; }
            catch (Throwable e) {
                throw ConfigErrors.problem(Secrets.redact("Configuring record transform for " + entry.getKey() + ": " + ConfigErrors.detail(e),
                        config.redactions()), Secrets.sanitize(e, config.redactions()));
            }
        }
    }

    public TopicSettings settings(String topic, TopicSettings settings) {
        TopicTransform transform = topics.get(topic);
        return settings.fingerprint(transform == null ? "" : transform.fingerprint);
    }

    public void validateOffline(Connection reader) throws SQLException {
        try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT topic,transform_fingerprint FROM \"$topics\" WHERE deleted_ms IS NULL")) {
            while (rows.next()) {
                String topic = rows.getString(1);
                requireFingerprint(topic, rows.getString(2));
            }
        }
    }

    /** Recheck on the query snapshot: another process may have rebuilt since this context opened. */
    public void validateOffline(Connection reader, String topic) throws SQLException {
        try (var p = reader.prepareStatement("SELECT transform_fingerprint FROM \"$topics\" WHERE topic=? AND deleted_ms IS NULL")) {
            p.setString(1, topic);
            try (var row = p.executeQuery()) { if (row.next()) requireFingerprint(topic, row.getString(1)); }
        }
    }

    private void requireFingerprint(String topic, String stored) throws SQLException {
        if (!stored.equals(settings(topic, config.topic(topic)).transformFingerprint()))
            throw ConfigErrors.problem("Offline cache has different record transform settings for topic " + topic + "; rebuild online first");
    }

    public static TransformSettings policy(String fingerprint) throws SQLException {
        if (fingerprint.isEmpty()) return TransformSettings.NONE;
        try { return JSON.readValue(fingerprint, Fingerprint.class).settings(); }
        catch (IOException e) { throw new SQLException(sentence("Reading cached record transform settings"), e); }
    }

    /** Runs only on a decoding owner, after the existing decoder finished both sides. */
    public SyncBatch.Record apply(ConsumerRecord<byte[], byte[]> raw, RecordDecoder.Decoded decoded) {
        TopicTransform transform = topics.get(raw.topic());
        if (transform == null) return new SyncBatch.Record(raw.offset(), raw.timestamp(), raw.key(), raw.headers(), decoded, raw.value() == null);
        byte[] identity = raw.key() == null ? null : transform.hash(raw.key());
        String failure = "Preparing record transform input failed";
        try {
            if (decoded.error() != null) return failed(raw, identity, "Record decoding failed");
            var headers = new ArrayList<RecordTransform.Header>();
            for (var header : raw.headers()) headers.add(new RecordTransform.Header(header.key(), header.value()));
            var record = new RecordTransform.Record(raw.topic(), raw.partition(), raw.offset(), raw.timestamp(),
                    decoded.key().schemaRef(), decoded.value().schemaRef(), read(decoded.key()), read(decoded.value()), headers);
            var result = record;
            if (transform.hook != null) {
                try { result = transform.hook.apply(record); }
                catch (VirtualMachineError e) { throw e; }
                catch (Throwable e) {
                    // Only user code's exception message is part of the public transform contract.
                    return failed(raw, identity, "Record transform: " + Secrets.redact(e.getMessage(), config.redactions()));
                }
            }
            if (result == null) return null;
            failure = "Applying record transforms failed";
            if (!Objects.equals(record.topic(), result.topic()) || record.partition() != result.partition()
                    || record.offset() != result.offset() || record.timestamp() != result.timestamp()
                    || !Objects.equals(record.keySchemaId(), result.keySchemaId()) || !Objects.equals(record.valueSchemaId(), result.valueSchemaId()))
                throw new IllegalArgumentException("Record transform changed read-only identity fields");
            result = transform.apply(result);
            failure = "Encoding transformed record failed";
            var changedHeaders = new RecordHeaders();
            if (result.headers() != null) for (var header : result.headers()) changedHeaders.add(header.name(), header.value());
            return new SyncBatch.Record(raw.offset(), raw.timestamp(), identity, changedHeaders,
                    new RecordDecoder.Decoded(part(decoded.key(), result.key()), part(decoded.value(), result.value()), null), raw.value() == null);
        } catch (VirtualMachineError e) { throw e; }
        catch (Throwable e) {
            // Driver/decoder exceptions may quote input or output, including fields the policy drops.
            return failed(raw, identity, failure);
        }
    }

    private static SyncBatch.Record failed(ConsumerRecord<byte[], byte[]> raw, byte[] identity, String error) {
        var empty = new RecordDecoder.Part(null, null, null, null, null);
        return new SyncBatch.Record(raw.offset(), raw.timestamp(), identity, null,
                new RecordDecoder.Decoded(empty, empty, error), raw.value() == null);
    }
    private static Object read(RecordDecoder.Part part) throws IOException {
        return part.json() == null ? null : JSON.readValue(part.json(), Object.class);
    }
    private static RecordDecoder.Part part(RecordDecoder.Part original, Object value) throws IOException {
        return new RecordDecoder.Part(value == null ? null : JSON.writeValueAsString(value), original.schemaRef(), original.schema(), null, original.messageIndex());
    }

    private static final class TopicTransform implements RecordTransform {
        private final TransformSettings settings;
        private final RecordTransform hook;
        private final ThreadLocal<Mac> mac;
        private final String fingerprint;

        TopicTransform(TransformSettings settings, RecordTransform hook, String secret) throws Exception {
            this.settings = settings; this.hook = hook;
            var key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac = ThreadLocal.withInitial(() -> {
                try { var hmac = Mac.getInstance("HmacSHA256"); hmac.init(key); return hmac; }
                catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
            });
            fingerprint = JSON.writeValueAsString(new Fingerprint(settings, hook == null ? "" : Objects.requireNonNull(hook.version(), "transform version"),
                    HexFormat.of().formatHex(hash("KafCacheQL record transforms".getBytes(StandardCharsets.UTF_8)))));
            mac.remove(); // Construction runs on the opening thread, not a decoding owner.
        }
        byte[] hash(byte[] value) { return mac.get().doFinal(value); }
        String mask(Object value) throws IOException { return HexFormat.of().formatHex(hash(JSON.writeValueAsBytes(value))); }

        @Override public Record apply(Record record) throws Exception {
            for (String path : settings.drop()) change(record, path, false);
            for (String path : settings.mask()) change(record, path, true);
            if (record.headers() != null) {
                var headers = new ArrayList<Header>();
                for (var header : record.headers()) {
                    if (settings.dropHeaders().contains(header.name())) continue;
                    headers.add(settings.maskHeaders().contains(header.name())
                            ? new Header(header.name(), mask(header.value()).getBytes(StandardCharsets.US_ASCII)) : header);
                }
                record.headers(headers);
            }
            return record;
        }

        private void change(Record record, String path, boolean mask) throws IOException {
            if (path.equals("_key")) { if (record.key() != null) record.key(mask ? mask(record.key()) : null); return; }
            if (path.equals("_value")) { if (record.value() != null) record.value(mask ? mask(record.value()) : null); return; }
            boolean key = path.startsWith("_key.");
            String[] parts = (key ? path.substring(5) : path).split("\\.");
            if (key) record.key(change(record.key(), parts, 0, mask));
            else record.value(change(record.value(), parts, 0, mask));
        }

        /** Copy only the changed path: hooks may return immutable JDK maps. */
        private Object change(Object node, String[] path, int at, boolean mask) throws IOException {
            if (!(node instanceof Map<?, ?> object) || !object.containsKey(path[at])) return node;
            Map<Object, Object> changed = new java.util.LinkedHashMap<>(object);
            if (at < path.length - 1) {
                Object child = change(object.get(path[at]), path, at + 1, mask);
                if (child == object.get(path[at])) return node;
                changed.put(path[at], child);
            } else if (mask) changed.put(path[at], mask(object.get(path[at])));
            else changed.remove(path[at]);
            return changed;
        }
    }
}
