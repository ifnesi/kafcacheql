package io.github.ifnesi.kafcacheql.config;

import java.util.Arrays;
import java.util.Optional;

/** How a topic is cached. */
public enum Mode {
    APPEND("append"), COMPACTED("compacted");

    private final String value;

    Mode(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    static Optional<Mode> parse(String value) {
        return Arrays.stream(values()).filter(m -> m.value.equals(value)).findFirst();
    }

    static String[] choices() {
        return Arrays.stream(values()).map(Mode::value).toArray(String[]::new);
    }
}
