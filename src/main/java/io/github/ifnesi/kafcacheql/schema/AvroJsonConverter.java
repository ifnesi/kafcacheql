package io.github.ifnesi.kafcacheql.schema;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

/** JSON representation defined by PLAN §7.2. */
public final class AvroJsonConverter {
    /** Bump whenever the stored JSON representation changes (PLAN §7.3). */
    public static final int VERSION = 4;

    private static final GenericData DATA = AvroSchemaUtils.getGenericData();
    private static final DateTimeFormatter TIME_MILLIS = time(3);
    private static final DateTimeFormatter TIME_MICROS = time(6);
    private static final DateTimeFormatter TIMESTAMP_MILLIS = timestamp(TIME_MILLIS);
    private static final DateTimeFormatter TIMESTAMP_MICROS = timestamp(TIME_MICROS);

    private AvroJsonConverter() {}

    /** Writes directly to the JSON generator, with no intermediate tree or copy of nested data. */
    public static String convert(Schema schema, Object value) throws IOException {
        StringWriter out = new StringWriter();
        try (JsonGenerator json = JacksonMapper.INSTANCE.getFactory().createGenerator(out)) {
            write(json, schema, value);
        }
        return out.toString();
    }

    private static void write(JsonGenerator json, Schema schema, Object value) throws IOException {
        // Confluent returns top-level bytes as byte[]; Avro's union resolution and logical conversions
        // use ByteBuffer. Normalise once, without copying the bytes, before either operation.
        if (value instanceof byte[] bytes) {
            value = ByteBuffer.wrap(bytes);
        }
        if (schema.getType() == Schema.Type.UNION) {
            write(json, schema.getTypes().get(DATA.resolveUnion(schema, value)), value);
            return;
        }
        if (value == null) {
            json.writeNull();
            return;
        }
        LogicalType logical = schema.getLogicalType();
        if (logical != null && writeLogical(json, schema, logical, value)) {
            return;
        }
        switch (schema.getType()) {
            case NULL -> json.writeNull();
            case BOOLEAN -> json.writeBoolean((Boolean) value);
            case INT -> json.writeNumber((Integer) value);
            case LONG -> json.writeNumber((Long) value);
            // Jackson quotes non-finite numbers, producing the strings required by §7.2.
            case FLOAT -> json.writeNumber((Float) value);
            case DOUBLE -> json.writeNumber((Double) value);
            case STRING, ENUM -> json.writeString(value.toString());
            case BYTES -> binary(json, (ByteBuffer) value);
            case FIXED -> json.writeBinary(((GenericFixed) value).bytes());
            case RECORD -> {
                IndexedRecord record = (IndexedRecord) value;
                json.writeStartObject();
                for (Schema.Field field : schema.getFields()) {
                    json.writeFieldName(field.name());
                    write(json, field.schema(), record.get(field.pos()));
                }
                json.writeEndObject();
            }
            case MAP -> {
                json.writeStartObject();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    json.writeFieldName(entry.getKey().toString());
                    write(json, schema.getValueType(), entry.getValue());
                }
                json.writeEndObject();
            }
            case ARRAY -> {
                json.writeStartArray();
                for (Object element : (Iterable<?>) value) {
                    write(json, schema.getElementType(), element);
                }
                json.writeEndArray();
            }
            case UNION -> throw new AssertionError("union resolved above");
        }
    }

    private static boolean writeLogical(JsonGenerator json, Schema schema, LogicalType logical, Object value)
            throws IOException {
        Conversion<?> conversion = DATA.getConversionFor(logical);
        if (conversion == null) {
            return false;
        }
        Object converted = conversion.getConvertedType().isInstance(value) ? value
                : Conversions.convertToLogicalType(value, schema, logical, conversion);
        switch (logical.getName()) {
            case "decimal" -> json.writeString(((BigDecimal) converted).toPlainString());
            case "uuid", "date" -> json.writeString(converted.toString());
            case "time-millis" -> json.writeString(TIME_MILLIS.format((TemporalAccessor) converted));
            case "time-micros" -> json.writeString(TIME_MICROS.format((TemporalAccessor) converted));
            case "timestamp-millis" -> json.writeString(TIMESTAMP_MILLIS.format((TemporalAccessor) converted) + 'Z');
            case "timestamp-micros" -> json.writeString(TIMESTAMP_MICROS.format((TemporalAccessor) converted) + 'Z');
            case "local-timestamp-millis" -> json.writeString(TIMESTAMP_MILLIS.format((TemporalAccessor) converted));
            case "local-timestamp-micros" -> json.writeString(TIMESTAMP_MICROS.format((TemporalAccessor) converted));
            default -> { return false; }
        }
        return true;
    }

    private static void binary(JsonGenerator json, ByteBuffer bytes) throws IOException {
        if (bytes.hasArray()) {
            json.writeBinary(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
        } else {
            try (var input = new ByteBufferBackedInputStream(bytes.duplicate())) {
                json.writeBinary(input, bytes.remaining());
            }
        }
    }

    private static DateTimeFormatter time(int digits) {
        return new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
                .appendFraction(ChronoField.NANO_OF_SECOND, digits, digits, true).toFormatter();
    }

    private static DateTimeFormatter timestamp(DateTimeFormatter time) {
        return new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T')
                .append(time).toFormatter().withZone(ZoneOffset.UTC);
    }
}
