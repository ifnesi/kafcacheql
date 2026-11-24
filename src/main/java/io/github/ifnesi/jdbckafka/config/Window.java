package io.github.ifnesi.jdbckafka.config;

import java.time.Duration;
import java.util.Optional;

/** The default window for append-topic queries without bounds: a duration, or all. */
public record Window(Optional<Duration> duration) {

    public static final Window ALL = new Window(Optional.empty());

    public static Window of(Duration duration) {
        return new Window(Optional.of(duration));
    }

    public boolean isAll() {
        return duration.isEmpty();
    }
}
