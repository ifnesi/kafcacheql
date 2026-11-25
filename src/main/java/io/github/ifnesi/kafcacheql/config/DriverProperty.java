package io.github.ifnesi.kafcacheql.config;

import java.util.Arrays;
import java.util.Optional;

/**
 * The driver's own properties. Every other property is a client setting, except
 * {@code topic.<name>.<setting>} overrides.
 */
public enum DriverProperty {
    CONFIG_FILE("config.file", null,
            "Properties file loaded before URL parameters and the Properties object, e.g. Confluent Cloud's client.properties."),
    CACHE_PATH("cache.path", null,
            "SQLite cache file. Defaults to ~/.kafcacheql/<cluster-id>.db; required when offline=true."),
    APPEND_DEFAULT_WINDOW("append.default.window", "1h",
            "Window for append-topic queries without an _offset or _timestamp bound: a duration (500ms, 90s, 15m, 1h, 2d) or all."),
    APPEND_DEFAULT_WINDOW_MAX_RECORDS("append.default.window.max.records", "10000",
            "Per-partition cap on the default window; whichever limit is smaller wins."),
    MAX_STALENESS_MS("max.staleness.ms", "0",
            "Skip the Kafka offsets check if the topic was checked within this time; 0 checks on every query."),
    OFFLINE("offline", "false",
            "Never contact Kafka or the Schema Registry; answer from the cache only.", "true", "false"),
    TOPICS_INCLUDE("topics.include", ".*",
            "Regex of topics listed by getTables()."),
    TOPICS_EXCLUDE("topics.exclude", "__.*|_confluent.*|_schemas",
            "Regex of topics hidden from getTables(); they stay queryable."),
    METADATA_SHOW_INTERNAL("metadata.show.internal", "false",
            "List the $ internal tables in getTables().", "true", "false"),
    CLIENT_ID("client.id", "kafcacheql",
            "Consumer client.id; the admin client gets <client.id>-admin."),
    SYNC_BATCH_RECORDS("sync.batch.records", "5000",
            "Records written per SQLite transaction while syncing.");

    private final String key;
    private final String defaultValue;
    private final String description;
    private final String[] choices;

    DriverProperty(String key, String defaultValue, String description, String... choices) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.description = description;
        this.choices = choices;
    }

    public String key() {
        return key;
    }

    /** The built-in default, or null when there is none. */
    public String defaultValue() {
        return defaultValue;
    }

    public String description() {
        return description;
    }

    /** Allowed values, or an empty array when any value of the right type is allowed. */
    public String[] choices() {
        return choices.clone();
    }

    public static Optional<DriverProperty> byKey(String key) {
        return Arrays.stream(values()).filter(p -> p.key.equals(key)).findFirst();
    }
}
