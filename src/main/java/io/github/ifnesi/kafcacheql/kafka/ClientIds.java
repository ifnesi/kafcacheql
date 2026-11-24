package io.github.ifnesi.kafcacheql.kafka;

import java.util.HashSet;
import java.util.Set;

/**
 * The client.ids in use in this JVM: {@code base}, then {@code base-2}, {@code base-3} while others are taken.
 * Kafka registers a JMX bean per client.id, so an ID stays reserved until its client has finished closing:
 * a client opened while another still closes under the same ID would lose its bean when the old one goes.
 */
final class ClientIds {

    private static final Set<String> RESERVED = new HashSet<>();

    private ClientIds() {}

    static synchronized String reserve(String base) {
        String id = base;
        for (int n = 2; RESERVED.contains(id); n++) {
            id = base + "-" + n;
        }
        RESERVED.add(id);
        return id;
    }

    static synchronized void release(String id) {
        RESERVED.remove(id);
    }
}
