package io.github.ifnesi.jdbckafka.kafka;

import java.util.Map;

/**
 * Each partition's end offset (the last stable offset, under read_committed) and when it was captured: the
 * time just before the request, so every record at or after an end offset was written after it (the caught-up time
 * that decides when a compacted topic must be rebuilt).
 */
public record EndOffsets(Map<Integer, Long> offsets, long capturedAtMs) {}
