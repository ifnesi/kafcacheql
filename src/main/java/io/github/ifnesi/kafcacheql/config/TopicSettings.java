package io.github.ifnesi.kafcacheql.config;

import java.util.Optional;
import java.util.OptionalLong;

/** A topic's {@code topic.<name>.<setting>} overrides; empty where not overridden. */
public record TopicSettings(
        Optional<Mode> mode,
        Optional<Format> keyFormat,
        Optional<Format> valueFormat,
        Optional<Window> appendDefaultWindow,
        OptionalLong appendDefaultWindowMaxRecords,
        OptionalLong deleteRetentionMs) {

    public static final TopicSettings NONE = new TopicSettings(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), OptionalLong.empty(), OptionalLong.empty());

    /** The settings, by the suffix that names them. */
    enum Setting {
        MODE("mode"),
        KEY_FORMAT("key.format"),
        VALUE_FORMAT("value.format"),
        APPEND_DEFAULT_WINDOW("append.default.window"),
        APPEND_DEFAULT_WINDOW_MAX_RECORDS("append.default.window.max.records"),
        DELETE_RETENTION_MS("delete.retention.ms");

        final String suffix;

        Setting(String suffix) {
            this.suffix = suffix;
        }
    }

    TopicSettings with(Setting setting, Object value) {
        return switch (setting) {
            case MODE -> new TopicSettings(Optional.of((Mode) value), keyFormat, valueFormat,
                    appendDefaultWindow, appendDefaultWindowMaxRecords, deleteRetentionMs);
            case KEY_FORMAT -> new TopicSettings(mode, Optional.of((Format) value), valueFormat,
                    appendDefaultWindow, appendDefaultWindowMaxRecords, deleteRetentionMs);
            case VALUE_FORMAT -> new TopicSettings(mode, keyFormat, Optional.of((Format) value),
                    appendDefaultWindow, appendDefaultWindowMaxRecords, deleteRetentionMs);
            case APPEND_DEFAULT_WINDOW -> new TopicSettings(mode, keyFormat, valueFormat,
                    Optional.of((Window) value), appendDefaultWindowMaxRecords, deleteRetentionMs);
            case APPEND_DEFAULT_WINDOW_MAX_RECORDS -> new TopicSettings(mode, keyFormat, valueFormat,
                    appendDefaultWindow, OptionalLong.of((Long) value), deleteRetentionMs);
            case DELETE_RETENTION_MS -> new TopicSettings(mode, keyFormat, valueFormat,
                    appendDefaultWindow, appendDefaultWindowMaxRecords, OptionalLong.of((Long) value));
        };
    }
}
