package io.github.ifnesi.kafcacheql.sync;

import java.sql.SQLException;

/** Internal signal to re-observe the whole query. Never applies an obsolete Kafka observation. */
public final class StaleGeneration extends SQLException {
    private static final long serialVersionUID = 1L;
    private final String topic;

    public StaleGeneration(String topic) {
        super("Stale cache generation for topic " + topic, "40001");
        this.topic = topic;
    }

    public String topic() { return topic; }
}
