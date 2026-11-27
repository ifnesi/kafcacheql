package io.github.ifnesi.kafcacheql.config;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/** Per-topic transform policy. Contains no secret. */
public record TransformSettings(List<String> drop, List<String> mask, List<String> dropHeaders,
                                List<String> maskHeaders, String className) {
    public static final TransformSettings NONE = new TransformSettings(List.of(), List.of(), List.of(), List.of(), "");
    public boolean enabled() { return !equals(NONE); }

    static List<String> list(String key, String value, boolean paths) throws SQLException {
        if (value.isBlank()) return List.of();
        var entries = Arrays.stream(value.split(",", -1)).map(String::trim).distinct().sorted().toList();
        for (String entry : entries) {
            if (entry.isEmpty() || paths && (entry.contains("*") || entry.contains("[") || entry.contains("]")
                    || Arrays.stream(entry.split("\\.", -1)).anyMatch(String::isEmpty)))
                throw ConfigErrors.problem("Invalid " + key + ": use comma-separated " + (paths ? "dot paths without wildcards or indexes" : "header names"));
        }
        return entries;
    }
}
