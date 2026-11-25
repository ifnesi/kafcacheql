package io.github.ifnesi.kafcacheql.cache;

import com.google.common.base.Ascii;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.ArraySchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.github.ifnesi.kafcacheql.schema.SchemaRegistry;
import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Typed projections from cached schemas or sampled JSON. This class never contacts the registry. */
public final class TypedViews {
    private static final Logger log = LoggerFactory.getLogger(TypedViews.class);
    private static final Cache<Input, Layout> LAYOUTS = CacheBuilder.newBuilder().maximumSize(1000).build();

    private TypedViews() {}

    /** Schema types are available to the later JDBC result/metadata wrappers as well as the renderer. */
    public record Column(String name, String expression, JDBCType type, String warning) {}
    public record Layout(List<Column> columns) {}
    private record Input(List<SchemaStore.StoredSchema> keys, List<SchemaStore.StoredSchema> values, List<Inferred> inferred) {}
    private record Inferred(String name, Schema.Type type, boolean conflict, boolean key) {}
    private record Version(String ref, Object schema) {}
    private record ViewInput(Ids ids, List<Inferred> inferred) {}
    private record Ids(Set<String> keys, Set<String> values) {
        String signature(List<Inferred> inferred) throws SQLException {
            try { return JacksonMapper.INSTANCE.writeValueAsString(inferred.isEmpty() ? List.of(keys, values) : List.of(keys, values, inferred)); }
            catch (IOException e) { throw new SQLException("Encoding view schema IDs", e); }
        }
    }

    /** Called by registration, batches, and pruning, inside the worker's guarded write transaction. */
    public static void refresh(Connection writer, long generation) throws SQLException {
        refresh(writer, generation, Set.of(), Set.of());
    }

    static void refresh(Connection writer, long generation, Set<String> keyIds, Set<String> valueIds) throws SQLException {
        var previous = StorageDdl.viewSignature(writer, generation);
        Ids ids = previous.isPresent() ? readSignature(previous.get()).ids() : new Ids(new TreeSet<>(), new TreeSet<>());
        ids.keys().addAll(keyIds); ids.values().addAll(valueIds);
        // The persisted layout is read under the same write lock as the batch, never remembered per process.
        // Indexed existence checks also discard IDs whose last row was replaced, tombstoned or pruned.
        retainPresent(writer, generation, "_key_schema_id", ids.keys());
        retainPresent(writer, generation, "_value_schema_id", ids.values());
        List<Inferred> inferred = infer(writer, generation, ids);
        String signature = ids.signature(inferred);
        // A change of IDs still regenerates a view whose projected columns happen to be identical.
        if (previous.filter(signature::equals).isPresent()) return;
        Layout layout = describe(writer, ids, inferred);
        StorageDdl.view(writer, generation, layout.columns().stream()
                .map(c -> c.expression() + " AS " + StorageDdl.quote(c.name())).toList(), signature);
    }

    public static Layout describe(Connection reader, long generation) throws SQLException {
        ViewInput input = viewInput(reader, generation);
        return describe(reader, input.ids(), input.inferred());
    }

    private static ViewInput viewInput(Connection reader, long generation) throws SQLException {
        return readSignature(StorageDdl.viewSignature(reader, generation)
                .orElseThrow(() -> new SQLException("Missing typed view for generation " + generation)));
    }

    private static ViewInput readSignature(String signature) throws SQLException {
        try {
            JsonNode input = JacksonMapper.INSTANCE.readTree(signature);
            Ids ids = new Ids(new TreeSet<>(List.of(JacksonMapper.INSTANCE.treeToValue(input.get(0), String[].class))),
                    new TreeSet<>(List.of(JacksonMapper.INSTANCE.treeToValue(input.get(1), String[].class))));
            List<Inferred> inferred = input.size() == 2 ? List.of()
                    : List.of(JacksonMapper.INSTANCE.treeToValue(input.get(2), Inferred[].class));
            return new ViewInput(ids, inferred);
        } catch (IOException e) { throw new SQLException("Reading cached view layout", e); }
    }

    /** A schema-less view can also result from unsupported or mixed record types; distinguish it for diagnostics. */
    public static boolean hasSchemas(Connection reader, long generation) throws SQLException {
        var ids = viewInput(reader, generation).ids();
        for (var refs : List.of(ids.keys(), ids.values())) {
            for (String ref : refs) if (SchemaStore.find(reader, ref).isPresent()) return true;
        }
        return false;
    }

    /** The full view layout, shared by JDBC results and database metadata. */
    public static List<Column> columns(Connection reader, long generation) throws SQLException {
        return columns(describe(reader, generation));
    }

    public static List<Column> columns(Layout layout) {
        var columns = new ArrayList<Column>();
        for (String name : StorageDdl.METADATA) {
            JDBCType type = switch (name) {
                case "_partition" -> JDBCType.INTEGER;
                case "_offset", "_timestamp" -> JDBCType.BIGINT;
                default -> JDBCType.VARCHAR;
            };
            columns.add(new Column(name, name, type, null));
        }
        columns.addAll(layout.columns());
        return List.copyOf(columns);
    }

    /** Called on the query's checked read snapshot, including when no sync write was necessary. */
    public static List<SQLWarning> warnings(Layout layout, String topic) {
        var warnings = new ArrayList<SQLWarning>();
        for (Column column : layout.columns()) {
            if (column.warning() == null) continue;
            String message = "Topic " + topic + ": " + column.warning();
            log.warn("{}", message);
            warnings.add(new SQLWarning(message, "01000"));
        }
        return warnings;
    }

    private static void retainPresent(Connection reader, long generation, String column, Set<String> refs) throws SQLException {
        if (refs.isEmpty()) return;
        try (var p = reader.prepareStatement("SELECT 1 FROM " + StorageDdl.quote(StorageDdl.tableName(generation))
                + " WHERE " + column + "=? LIMIT 1")) {
            for (var it = refs.iterator(); it.hasNext();) {
                p.setString(1, it.next());
                try (var rows = p.executeQuery()) { if (!rows.next()) it.remove(); }
            }
        }
    }

    private static Layout describe(Connection reader, Ids ids, List<Inferred> inferred) throws SQLException {
        Input input = new Input(schemas(reader, ids.keys()), schemas(reader, ids.values()), inferred);
        Layout cached = LAYOUTS.getIfPresent(input);
        if (cached != null) return cached;
        try {
            List<Version> keys = versions(input.keys()), values = versions(input.values());
            var columns = new ArrayList<Column>();
            if (!multipleRecords(keys) && !multipleRecords(values)) {
                Set<String> names = new HashSet<>(StorageDdl.METADATA);
                append(columns, names, keys, true);
                appendInferred(columns, names, inferred, true);
                append(columns, names, values, false);
                appendInferred(columns, names, inferred, false);
            }
            Layout result = new Layout(List.copyOf(columns));
            LAYOUTS.put(input, result);
            return result;
        } catch (RuntimeException e) { throw new SQLException("Reading cached schemas for a typed view", e); }
    }

    /** Sample only on the worker's refresh path. Readers use the descriptors stored with the view. */
    private static List<Inferred> infer(Connection reader, long generation, Ids ids) throws SQLException {
        if (!ids.keys().isEmpty() && !ids.values().isEmpty()) return List.of();
        var keys = new TreeMap<String, EnumSet<Schema.Type>>(); var values = new TreeMap<String, EnumSet<Schema.Type>>();
        try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT _key,_value FROM "
                + StorageDdl.quote(StorageDdl.tableName(generation)) + " ORDER BY _timestamp DESC,_partition DESC,_offset DESC LIMIT 1000")) {
            while (rows.next()) {
                if (ids.keys().isEmpty()) sample(keys, rows.getString(1));
                if (ids.values().isEmpty()) sample(values, rows.getString(2));
            }
        } catch (IOException e) { throw new SQLException("Sampling cached JSON for a typed view", e); }
        var fields = new ArrayList<Inferred>();
        for (boolean key : List.of(true, false)) {
            for (var field : (key ? keys : values).entrySet()) {
                var types = field.getValue();
                if (types.contains(Schema.Type.DOUBLE)) types.remove(Schema.Type.LONG);
                fields.add(new Inferred(field.getKey(), types.iterator().next(), types.size() > 1, key));
            }
        }
        return List.copyOf(fields);
    }

    private static void sample(Map<String, EnumSet<Schema.Type>> fields, String json) throws IOException {
        if (json == null) return;
        JsonNode object = JacksonMapper.INSTANCE.readTree(json);
        if (!object.isObject()) return;
        for (var field : object.properties()) {
            JsonNode value = field.getValue();
            Schema.Type type = switch (value.getNodeType()) {
                case BOOLEAN -> Schema.Type.BOOLEAN;
                case NUMBER -> value.isIntegralNumber() ? Schema.Type.LONG : Schema.Type.DOUBLE;
                case STRING -> Schema.Type.STRING;
                case OBJECT -> Schema.Type.RECORD;
                case ARRAY -> Schema.Type.ARRAY;
                case NULL -> Schema.Type.NULL;
                default -> throw new IllegalArgumentException("Unexpected JSON type " + value.getNodeType());
            };
            fields.computeIfAbsent(field.getKey(), ignored -> EnumSet.noneOf(Schema.Type.class)).add(type);
        }
    }

    private static void appendInferred(List<Column> columns, Set<String> names, List<Inferred> fields, boolean key) throws SQLException {
        for (var field : fields) {
            if (field.key() != key) continue;
            String base = key ? "key_" + field.name() : field.name();
            String warning = field.conflict() ? "Field " + base + " has conflicting types in the cached JSON sample; exposed as text without conversion" : null;
            add(columns, names, base, field.name(), key, field.conflict() ? JDBCType.VARCHAR : type(field.type()),
                    field.conflict() || nested(field.type()), warning);
        }
    }

    private static List<SchemaStore.StoredSchema> schemas(Connection reader, Set<String> ids) throws SQLException {
        var result = new ArrayList<SchemaStore.StoredSchema>();
        for (String id : ids) SchemaStore.find(reader, id).filter(s -> AvroSchema.TYPE.equals(s.type()) || JsonSchema.TYPE.equals(s.type())).ifPresent(result::add);
        return List.copyOf(result);
    }

    private static List<Version> versions(List<SchemaStore.StoredSchema> schemas) {
        return schemas.stream().map(s -> new Version(s.ref(), nullable(JsonSchema.TYPE.equals(s.type())
                ? SchemaRegistry.parseJson(s).rawSchema() : SchemaRegistry.parseAvro(s).rawSchema()))).toList();
    }

    private static boolean multipleRecords(List<Version> versions) {
        return versions.stream().map(Version::schema).filter(Schema.class::isInstance).map(Schema.class::cast)
                .filter(s -> s.getType() == Schema.Type.RECORD).map(Schema::getFullName).distinct().limit(2).count() > 1;
    }

    private static void append(List<Column> columns, Set<String> names, List<Version> versions, boolean key) throws SQLException {
        Map<String, List<Version>> fields = new LinkedHashMap<>();
        for (Version version : versions) {
            Object schema = version.schema();
            if (schema instanceof Schema avro && avro.getType() == Schema.Type.RECORD) {
                for (Schema.Field field : avro.getFields()) fields.computeIfAbsent(field.name(), ignored -> new ArrayList<>())
                        .add(new Version(version.ref(), nullable(field.schema())));
            } else if (schema instanceof ObjectSchema json) {
                for (var field : json.getPropertySchemas().entrySet()) fields.computeIfAbsent(field.getKey(), ignored -> new ArrayList<>())
                        .add(new Version(version.ref(), nullable(field.getValue())));
            } else if (key) fields.computeIfAbsent("", ignored -> new ArrayList<>()).add(version);
        }
        for (var entry : fields.entrySet()) {
            String field = entry.getKey();
            String base = key ? field.isEmpty() ? "key" : "key_" + field : field;
            List<Version> types = entry.getValue();
            Object widest = types.getFirst().schema();
            boolean conflict = false;
            for (int i = 1; i < types.size(); i++) {
                Object other = types.get(i).schema();
                if (agrees(widest, other)) {
                    if (accepts(other, widest)) widest = other;
                } else conflict = true;
            }
            JDBCType type = conflict ? JDBCType.VARCHAR : type(widest);
            String warning = conflict ? "Field " + base + " has conflicting types in schema IDs "
                    + types.stream().map(Version::ref).toList() + "; exposed as text without conversion" : null;
            add(columns, names, base, field.isEmpty() ? null : field, key, type, conflict || nested(kind(widest)), warning);
        }
    }

    private static void add(List<Column> columns, Set<String> names, String base, String field, boolean key,
                            JDBCType type, boolean json, String warning) throws SQLException {
        String name = base;
        for (int suffix = 1; !names.add(Ascii.toLowerCase(name)); suffix++) name = base + '_' + suffix;
        String path;
        try { path = field == null ? "$" : "$." + JacksonMapper.INSTANCE.writeValueAsString(field); }
        catch (IOException e) { throw new SQLException("Encoding JSON field path", e); }
        String expression = (key ? "_key" : "_value") + (json ? " -> " : " ->> ") + "'" + path.replace("'", "''") + "'";
        if (type == JDBCType.NUMERIC) expression = "CAST(" + expression + " AS NUMERIC)";
        columns.add(new Column(name, expression, type, warning));
    }

    private static Object nullable(Object value) {
        if (value instanceof Schema schema) {
            if (schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2) {
                var types = schema.getTypes();
                if (types.getFirst().getType() == Schema.Type.NULL) return types.getLast();
                if (types.getLast().getType() == Schema.Type.NULL) return types.getFirst();
            }
        } else if (value instanceof ReferenceSchema reference) {
            return nullable(reference.getReferredSchema());
        } else if (value instanceof CombinedSchema combined) {
            // Everit represents nullable types as anyOf, and a type plus enum as allOf.
            var types = combined.getSubschemas().stream().filter(s -> combined.getCriterion() == CombinedSchema.ALL_CRITERION
                    ? !(s instanceof EnumSchema) : !(s instanceof NullSchema)).toList();
            if (types.size() == 1) return nullable(types.getFirst());
        }
        return value;
    }

    private static boolean agrees(Object a, Object b) {
        if (a instanceof Schema avroA && b instanceof Schema avroB) {
            // Logical types change the JSON representation, even when their Avro storage types agree.
            if (!java.util.Objects.equals(avroA.getProp("logicalType"), avroB.getProp("logicalType"))) return false;
            if (!java.util.Objects.equals(avroA.getObjectProp("scale"), avroB.getObjectProp("scale"))) return false;
            return avroA.equals(avroB) || accepts(a, b) || accepts(b, a);
        }
        return accepts(a, b) || accepts(b, a);
    }

    private static boolean accepts(Object reader, Object writer) {
        if (reader instanceof Schema avroReader && writer instanceof Schema avroWriter) {
            return SchemaCompatibility.checkReaderWriterCompatibility(avroReader, avroWriter).getType()
                    == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
        }
        return kind(reader) == kind(writer) && type(reader) == type(writer)
                || type(reader) == JDBCType.DOUBLE && type(writer) == JDBCType.BIGINT;
    }

    private static Schema.Type kind(Object schema) {
        return switch (schema) {
            case Schema avro -> avro.getType();
            case ObjectSchema ignored -> Schema.Type.RECORD;
            case ArraySchema ignored -> Schema.Type.ARRAY;
            case NumberSchema number -> number.requiresInteger() ? Schema.Type.LONG : Schema.Type.DOUBLE;
            case BooleanSchema ignored -> Schema.Type.BOOLEAN;
            case StringSchema ignored -> Schema.Type.STRING;
            case EnumSchema ignored -> Schema.Type.STRING;
            case NullSchema ignored -> Schema.Type.NULL;
            default -> Schema.Type.UNION;
        };
    }

    private static boolean nested(Schema.Type type) {
        return switch (type) { case RECORD, ARRAY, MAP, UNION -> true; default -> false; };
    }

    private static JDBCType type(Object schema) {
        if (schema instanceof Schema avro && avro.getLogicalType() != null) {
            return avro.getLogicalType().getName().equals("decimal") ? JDBCType.NUMERIC : JDBCType.VARCHAR;
        }
        return type(kind(schema));
    }

    private static JDBCType type(Schema.Type type) {
        return switch (type) {
            case BOOLEAN -> JDBCType.BOOLEAN;
            case INT, LONG -> JDBCType.BIGINT;
            case FLOAT, DOUBLE -> JDBCType.DOUBLE;
            default -> JDBCType.VARCHAR;
        };
    }
}
