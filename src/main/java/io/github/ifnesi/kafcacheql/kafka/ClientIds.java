package io.github.ifnesi.kafcacheql.kafka;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The client.ids in use in this JVM: {@code base}, then {@code base-2}, {@code base-3} while others are taken.
 * Kafka registers a JMX bean per client.id, so an ID stays reserved until its client has finished closing:
 * a client opened while another still closes under the same ID would lose its bean when the old one goes.
 */
public final class ClientIds {

    /** Kafka uses separate JMX domains for admin clients and consumers. */
    public enum Kind { ADMIN, CONSUMER }
    private static final Map<Kind, Set<String>> RESERVED = new EnumMap<>(Kind.class);

    static { for (Kind kind : Kind.values()) RESERVED.put(kind, new HashSet<>()); }

    private ClientIds() {}

    public static synchronized String reserve(String base) {
        return reserve(Kind.ADMIN, base);
    }

    public static synchronized String reserve(Kind kind, String base) {
        Set<String> reserved = RESERVED.get(kind);
        String id = base;
        for (int n = 2; reserved.contains(id); n++) {
            id = base + "-" + n;
        }
        reserved.add(id);
        return id;
    }

    public static synchronized void release(String id) {
        release(Kind.ADMIN, id);
    }

    public static synchronized void release(Kind kind, String id) {
        RESERVED.get(kind).remove(id);
    }
}
