package io.github.ifnesi.kafcacheql.transform;

import java.util.List;

/** A stateless, thread-safe transform, shared by the decoding threads of one cache. */
public interface RecordTransform {
    /** Return the changed record, or null to skip it. Identity fields must not change. */
    Record apply(Record record) throws Exception;

    /** Change this when the transform's output changes, so existing cached records are rebuilt. */
    default String version() { return "1"; }

    /** Header values are bytes; duplicate names and null values are preserved. */
    record Header(String name, byte[] value) {}

    /** Key and value use only Map<String,Object>, List<Object>, String, Long, Double, Boolean, byte[] or null. */
    final class Record {
        private final String topic;
        private final int partition;
        private final long offset, timestamp;
        private final String keySchemaId, valueSchemaId;
        private Object key, value;
        private List<Header> headers;

        public Record(String topic, int partition, long offset, long timestamp, String keySchemaId,
                      String valueSchemaId, Object key, Object value, List<Header> headers) {
            this.topic = topic; this.partition = partition; this.offset = offset; this.timestamp = timestamp;
            this.keySchemaId = keySchemaId; this.valueSchemaId = valueSchemaId;
            this.key = key; this.value = value; this.headers = headers;
        }
        public String topic() { return topic; }
        public int partition() { return partition; }
        public long offset() { return offset; }
        public long timestamp() { return timestamp; }
        public String keySchemaId() { return keySchemaId; }
        public String valueSchemaId() { return valueSchemaId; }
        public Object key() { return key; }
        public Object value() { return value; }
        public List<Header> headers() { return headers; }
        public void key(Object key) { this.key = key; }
        public void value(Object value) { this.value = value; }
        public void headers(List<Header> headers) { this.headers = headers; }
    }
}
