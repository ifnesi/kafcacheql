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
        OptionalLong deleteRetentionMs,
        TransformSettings transforms,
        String transformFingerprint) {

    public static final TopicSettings NONE = new TopicSettings(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), OptionalLong.empty(), OptionalLong.empty(), TransformSettings.NONE, "");

    public TopicSettings fingerprint(String value) {
        return new TopicSettings(mode, keyFormat, valueFormat, appendDefaultWindow, appendDefaultWindowMaxRecords,
                deleteRetentionMs, transforms, value);
    }

    /** The settings, by the suffix that names them. */
    enum Setting {
        MODE("mode"),
        KEY_FORMAT("key.format"),
        VALUE_FORMAT("value.format"),
        APPEND_DEFAULT_WINDOW("append.default.window"),
        APPEND_DEFAULT_WINDOW_MAX_RECORDS("append.default.window.max.records"),
        DELETE_RETENTION_MS("delete.retention.ms"),
        DROP("drop"), MASK("mask"), DROP_HEADERS("drop.headers"), MASK_HEADERS("mask.headers"), TRANSFORM("transform");

        final String suffix;

        Setting(String suffix) {
            this.suffix = suffix;
        }
    }

    TopicSettings with(Setting setting, Object value) {
        if (setting == Setting.DROP || setting == Setting.MASK || setting == Setting.DROP_HEADERS
                || setting == Setting.MASK_HEADERS || setting == Setting.TRANSFORM) {
            @SuppressWarnings("unchecked")
            var list = value instanceof java.util.List<?> ? (java.util.List<String>) value : java.util.List.<String>of();
            var t = transforms;
            return new TopicSettings(mode, keyFormat, valueFormat, appendDefaultWindow, appendDefaultWindowMaxRecords,
                    deleteRetentionMs, new TransformSettings(setting == Setting.DROP ? list : t.drop(),
                    setting == Setting.MASK ? list : t.mask(), setting == Setting.DROP_HEADERS ? list : t.dropHeaders(),
                    setting == Setting.MASK_HEADERS ? list : t.maskHeaders(), setting == Setting.TRANSFORM ? (String) value : t.className()), "");
        }
        return switch (setting) {
            case MODE -> new TopicSettings(Optional.of((Mode) value), keyFormat, valueFormat,
                    appendDefaultWindow, appendDefaultWindowMaxRecords, deleteRetentionMs, transforms, transformFingerprint);
            case KEY_FORMAT -> new TopicSettings(mode, Optional.of((Format) value), valueFormat,
                    appendDefaultWindow, appendDefaultWindowMaxRecords, deleteRetentionMs, transforms, transformFingerprint);
            case VALUE_FORMAT -> new TopicSettings(mode, keyFormat, Optional.of((Format) value),
                    appendDefaultWindow, appendDefaultWindowMaxRecords, deleteRetentionMs, transforms, transformFingerprint);
            case APPEND_DEFAULT_WINDOW -> new TopicSettings(mode, keyFormat, valueFormat,
                    Optional.of((Window) value), appendDefaultWindowMaxRecords, deleteRetentionMs, transforms, transformFingerprint);
            case APPEND_DEFAULT_WINDOW_MAX_RECORDS -> new TopicSettings(mode, keyFormat, valueFormat,
                    appendDefaultWindow, OptionalLong.of((Long) value), deleteRetentionMs, transforms, transformFingerprint);
            case DELETE_RETENTION_MS -> new TopicSettings(mode, keyFormat, valueFormat,
                    appendDefaultWindow, appendDefaultWindowMaxRecords, OptionalLong.of((Long) value), transforms, transformFingerprint);
            default -> throw new AssertionError(setting);
        };
    }
}
