package io.github.ifnesi.jdbckafka.config;

import java.util.Arrays;
import java.util.Optional;

/** How a key or value is decoded. */
public enum Format {
    AUTO("auto"), AVRO("avro"), JSON_SR("json-sr"), PROTOBUF("protobuf"), JSON("json"), STRING("string"), BYTES("bytes");

    private final String value;

    Format(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    static Optional<Format> parse(String value) {
        return Arrays.stream(values()).filter(f -> f.value.equals(value)).findFirst();
    }

    static String[] choices() {
        return Arrays.stream(values()).map(Format::value).toArray(String[]::new);
    }
}
