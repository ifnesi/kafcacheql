package io.github.ifnesi.kafcacheql.cache;

import com.google.common.base.Ascii;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.github.ifnesi.kafcacheql.schema.SchemaRegistry;
import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Schema-derived projections. All inputs are durable cache rows; this class never contacts the registry. */
public final class TypedViews {
    private static final Logger log = LoggerFactory.getLogger(TypedViews.class);
    private static final Cache<Input, Layout> LAYOUTS = CacheBuilder.newBuilder().maximumSize(1000).build();

    private TypedViews() {}

    /** Schema types are available to the later JDBC result/metadata wrappers as well as the renderer. */
    public record Column(String name, String expression, JDBCType type, String warning) {}
    public record Layout(List<Column> columns) {}
    private record Input(List<SchemaStore.StoredSchema> keys, List<SchemaStore.StoredSchema> values) {}
    private record Version(String ref, Schema schema) {}
    private record Ids(Set<String> keys, Set<String> values) {
        String signature() throws SQLException {
            try { return JacksonMapper.INSTANCE.writeValueAsString(List.of(keys, values)); }
            catch (IOException e) { throw new SQLException("Encoding view schema IDs", e); }
        }
    }

    /** Called by registration, batches, and pruning, inside the worker's guarded write transaction. */
    public static void refresh(Connection writer, long generation) throws SQLException {
        Ids ids = ids(writer, generation);
        String signature = ids.signature();
        // A change of IDs still regenerates a view whose projected columns happen to be identical.
        if (StorageDdl.viewMatches(writer, generation, signature)) return;
        Layout layout = describe(writer, ids);
        StorageDdl.view(writer, generation, layout.columns().stream()
                .map(c -> c.expression() + " AS " + StorageDdl.quote(c.name())).toList(), signature);
    }

    public static Layout describe(Connection reader, long generation) throws SQLException {
        return describe(reader, ids(reader, generation));
    }

    /** A schema-less view can also result from unsupported or mixed record types; distinguish it for diagnostics. */
    public static boolean hasSchemas(Connection reader, long generation) throws SQLException {
        var ids = ids(reader, generation);
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

    private static Ids ids(Connection reader, long generation) throws SQLException {
        Set<String> keys = new TreeSet<>(), values = new TreeSet<>();
        try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT DISTINCT _key_schema_id, _value_schema_id FROM "
                + StorageDdl.quote(StorageDdl.tableName(generation)))) {
            while (rows.next()) {
                String key = rows.getString(1), value = rows.getString(2);
                if (key != null) keys.add(key);
                if (value != null) values.add(value);
            }
        }
        return new Ids(keys, values);
    }

    private static Layout describe(Connection reader, Ids ids) throws SQLException {
        Input input = new Input(schemas(reader, ids.keys()), schemas(reader, ids.values()));
        Layout cached = LAYOUTS.getIfPresent(input);
        if (cached != null) return cached;
        try {
            List<Version> keys = versions(input.keys()), values = versions(input.values());
            var columns = new ArrayList<Column>();
            if (!multipleRecords(keys) && !multipleRecords(values)) {
                Set<String> names = new HashSet<>(StorageDdl.METADATA);
                append(columns, names, keys, true);
                append(columns, names, values, false);
            }
            Layout result = new Layout(List.copyOf(columns));
            LAYOUTS.put(input, result);
            return result;
        } catch (RuntimeException e) { throw new SQLException("Reading cached Avro schemas for a typed view", e); }
    }

    private static List<SchemaStore.StoredSchema> schemas(Connection reader, Set<String> ids) throws SQLException {
        var result = new ArrayList<SchemaStore.StoredSchema>();
        for (String id : ids) SchemaStore.find(reader, id).filter(s -> AvroSchema.TYPE.equals(s.type())).ifPresent(result::add);
        return List.copyOf(result);
    }

    private static List<Version> versions(List<SchemaStore.StoredSchema> schemas) {
        return schemas.stream().map(s -> new Version(s.ref(), nullable(SchemaRegistry.parseAvro(s).rawSchema()))).toList();
    }

    private static boolean multipleRecords(List<Version> versions) {
        return versions.stream().map(Version::schema).filter(s -> s.getType() == Schema.Type.RECORD)
                .map(Schema::getFullName).distinct().limit(2).count() > 1;
    }

    private static void append(List<Column> columns, Set<String> names, List<Version> versions, boolean key) {
        Map<String, List<Version>> fields = new LinkedHashMap<>();
        for (Version version : versions) {
            Schema schema = version.schema();
            if (schema.getType() == Schema.Type.RECORD) {
                for (Schema.Field field : schema.getFields()) fields.computeIfAbsent(field.name(), ignored -> new ArrayList<>())
                        .add(new Version(version.ref(), nullable(field.schema())));
            } else if (key) fields.computeIfAbsent("", ignored -> new ArrayList<>()).add(version);
        }
        for (var entry : fields.entrySet()) {
            String field = entry.getKey();
            String base = key ? field.isEmpty() ? "key" : "key_" + field : field;
            String name = base;
            for (int suffix = 1; !names.add(Ascii.toLowerCase(name)); suffix++) name = base + '_' + suffix;
            List<Version> types = entry.getValue();
            Schema widest = types.getFirst().schema();
            boolean conflict = false;
            for (int i = 1; i < types.size(); i++) {
                Schema other = types.get(i).schema();
                if (agrees(widest, other)) {
                    if (accepts(other, widest)) widest = other;
                } else conflict = true;
            }
            JDBCType type = conflict ? JDBCType.VARCHAR : type(widest);
            boolean json = conflict || nested(widest);
            String path = field.isEmpty() ? "$" : "$.\"" + field + "\"";
            String expression = (key ? "_key" : "_value") + (json ? " -> " : " ->> ") + "'" + path + "'";
            if (type == JDBCType.NUMERIC) expression = "CAST(" + expression + " AS NUMERIC)";
            String warning = conflict ? "Field " + base + " has conflicting types in schema IDs "
                    + types.stream().map(Version::ref).toList() + "; exposed as text without conversion" : null;
            columns.add(new Column(name, expression, type, warning));
        }
    }

    private static Schema nullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2) {
            var types = schema.getTypes();
            if (types.getFirst().getType() == Schema.Type.NULL) return types.getLast();
            if (types.getLast().getType() == Schema.Type.NULL) return types.getFirst();
        }
        return schema;
    }

    private static boolean agrees(Schema a, Schema b) {
        // Logical types change the JSON representation, even when their Avro storage types agree.
        if (!java.util.Objects.equals(a.getProp("logicalType"), b.getProp("logicalType"))) return false;
        if (!java.util.Objects.equals(a.getObjectProp("scale"), b.getObjectProp("scale"))) return false;
        return a.equals(b) || accepts(a, b) || accepts(b, a);
    }

    private static boolean accepts(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType()
                == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
    }

    private static boolean nested(Schema schema) {
        return switch (schema.getType()) { case RECORD, ARRAY, MAP, UNION -> true; default -> false; };
    }

    private static JDBCType type(Schema schema) {
        if (schema.getLogicalType() != null) {
            return schema.getLogicalType().getName().equals("decimal") ? JDBCType.NUMERIC : JDBCType.VARCHAR;
        }
        return switch (schema.getType()) {
            case BOOLEAN -> JDBCType.BOOLEAN;
            case INT, LONG -> JDBCType.BIGINT;
            case FLOAT, DOUBLE -> JDBCType.DOUBLE;
            default -> JDBCType.VARCHAR;
        };
    }
}
