package io.github.ifnesi.jdbckafka.kafka;

import java.util.Map;

/**
 * Each partition's end offset (the last stable offset, under read_committed) and when it was captured: the
 * time just before the request.
 *
 * <p>The log cleaner never cleans at or past the last stable offset, which only moves forward. So nothing at or
 * after an end offset had been cleaned when it was captured, whatever it is: a later record, a record of an
 * open transaction, or a record an open transaction holds back. Since Kafka 3.1 (KIP-534) a tombstone's
 * delete.retention.ms counts from its first cleaning, so none of them can be removed before capturedAtMs +
 * delete.retention.ms. On older brokers it can count from its segment's last write, at most the age of the
 * oldest open transaction earlier; the rebuild rule therefore subtracts transaction.max.timeout.ms (0007a).
 */
public record EndOffsets(Map<Integer, Long> offsets, long capturedAtMs) {}
