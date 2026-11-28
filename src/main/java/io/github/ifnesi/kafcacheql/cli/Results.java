package io.github.ifnesi.kafcacheql.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.jline.utils.AttributedString;

/** Rendering belongs to the JDBC client, not the driver. */
final class Results {
    enum Mode {
        TABLE, LINE, JSON;

        static Mode parse(String name) {
            try { return valueOf(name.toUpperCase(Locale.ROOT)); }
            catch (IllegalArgumentException invalid) {
                throw new IllegalArgumentException("Mode must be table, line or json.");
            }
        }

        @Override public String toString() { return name().toLowerCase(Locale.ROOT); }
    }

    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final java.util.HexFormat HEX = java.util.HexFormat.of();
    private static final Set<String> JSON_COLUMNS = Set.of("_key", "_value", "_headers");
    private boolean lineRowPrinted;

    void print(ResultSet rows, Mode mode, boolean interactive) throws SQLException, IOException {
        var metadata = rows.getMetaData();
        int count = metadata.getColumnCount();
        var columns = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) columns.add(metadata.getColumnLabel(i));
        if (mode == Mode.JSON) {
            try (var out = JSON.getFactory().createGenerator(System.out)
                    .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)) {
                out.writeStartArray();
                while (rows.next()) {
                    out.writeStartObject();
                    for (int i = 1; i <= count; i++) {
                        String name = columns.get(i - 1);
                        out.writeFieldName(name);
                        if (JSON_COLUMNS.contains(name)) {
                            jsonValue(out, rows.getString(i));
                        } else {
                            out.writeObject(rows.getObject(i));
                        }
                    }
                    out.writeEndObject();
                }
                out.writeEndArray();
            }
            System.out.println();
        } else {
            columns.replaceAll(Results::cell);
            int[] widths = columns.stream().mapToInt(Results::width).toArray();
            if (mode == Mode.LINE) {
                if (interactive) lineRowPrinted = false;
                int widest = java.util.Arrays.stream(widths).max().orElse(0);
                while (rows.next()) {
                    if (lineRowPrinted) System.out.println();
                    lineRowPrinted = true;
                    for (int i = 1; i <= count; i++) {
                        System.out.println(" ".repeat(widest - widths[i - 1]) + columns.get(i - 1)
                                + ": " + cell(rows.getString(i)));
                    }
                }
                return;
            }
            var values = new ArrayList<List<String>>();
            while (rows.next()) {
                var row = new ArrayList<String>(count);
                for (int i = 1; i <= count; i++) {
                    String value = cell(rows.getString(i));
                    widths[i - 1] = Math.max(widths[i - 1], width(value));
                    row.add(value);
                }
                values.add(row);
            }
            tableRow(columns, widths);
            System.out.println(String.join("-+-", java.util.Arrays.stream(widths).mapToObj(n -> "-".repeat(n)).toList()));
            for (var row : values) tableRow(row, widths);
        }
    }

    private static void jsonValue(JsonGenerator out, String value) throws IOException {
        if (value == null) { out.writeNull(); return; }
        JsonNode parsed;
        try { parsed = JSON.readTree(value); }
        catch (JsonProcessingException invalid) { out.writeString(value); return; }
        if (parsed.isMissingNode()) out.writeString(value);
        else out.writeTree(parsed);
    }

    private static String cell(String value) {
        if (value == null) return "NULL";
        var escaped = new StringBuilder(value.length());
        value.codePoints().forEach(c -> {
            switch (c) {
                case '\r' -> escaped.append("\\r");
                case '\n' -> escaped.append("\\n");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (Character.isISOControl(c)) escaped.append("\\x").append(HEX.toHexDigits((byte) c));
                    else escaped.appendCodePoint(c);
                }
            }
        });
        return escaped.toString();
    }

    private static int width(String value) { return new AttributedString(value).columnLength(); }
    private static void tableRow(List<String> row, int[] widths) {
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) System.out.print(" | ");
            String value = row.get(i);
            System.out.print(value + " ".repeat(widths[i] - width(value)));
        }
        System.out.println();
    }
}
