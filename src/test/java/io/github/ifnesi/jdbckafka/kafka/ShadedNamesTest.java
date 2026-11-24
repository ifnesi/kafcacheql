package io.github.ifnesi.jdbckafka.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Run unshaded, so the prefix is passed in. */
class ShadedNamesTest {

    private static final String P = "io.github.ifnesi.jdbckafka.shaded.";
    private static final String PLAIN = "org.apache.kafka.common.security.plain.PlainLoginModule";

    @Test
    void unshadedThereIsNoPrefix() {
        assertEquals("", ShadedNames.PREFIX);
        assertEquals("", ShadedNames.prefixOf("org.apache.kafka.clients.CommonClientConfigs"));
        assertEquals(P, ShadedNames.prefixOf(P + "org.apache.kafka.clients.CommonClientConfigs"));
    }

    @Test
    void jaasRewritesOnlyTheLoginModuleAndLeavesItsOptionsByteForByte() {
        String options = " required username='k' password='s3cr3t org.apache.kafka. ;=\"x\"';";
        assertEquals(P + PLAIN + options, ShadedNames.jaas(PLAIN + options, P));
        assertEquals("  \t" + P + PLAIN + options, ShadedNames.jaas("  \t" + PLAIN + options, P));
        assertEquals(P + PLAIN + ";", ShadedNames.jaas(PLAIN + ";", P));
    }

    @Test
    void jaasWithAnotherLoginModuleIsUnchanged() {
        String krb = "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true;";
        assertEquals(krb, ShadedNames.jaas(krb, P));
    }

    @Test
    void classNamesAndLists() {
        assertEquals(P + "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                ShadedNames.className("io.confluent.kafka.serializers.subject.RecordNameStrategy", P));
        assertEquals(" " + P + "org.apache.kafka.X ", ShadedNames.className(" org.apache.kafka.X ", P));
        assertEquals("com.example.MyHandler", ShadedNames.className("com.example.MyHandler", P));
        assertEquals(P + "org.apache.kafka.A, com.example.B," + P + "io.confluent.C",
                ShadedNames.classList("org.apache.kafka.A, com.example.B,io.confluent.C", P));
    }

    @Test
    void whichSettingsAreRewritten() {
        Map<String, String> in = new LinkedHashMap<>();
        in.put("sasl.jaas.config", PLAIN + " required;");
        in.put("sasl.login.callback.handler.class", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        in.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");
        in.put("value.schema.id.deserializer", "io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer");
        in.put("interceptor.classes", "org.apache.kafka.A,com.example.B");
        in.put("metric.reporters", "org.apache.kafka.common.metrics.JmxReporter");
        in.put("config.providers.file.class", "org.apache.kafka.common.config.provider.FileConfigProvider");
        in.put("bootstrap.servers", "org.apache.kafka.example:9092");
        in.put("security.protocol", "SASL_SSL");

        Map<String, String> out = ShadedNames.rewrite(in, P);
        assertEquals(P + PLAIN + " required;", out.get("sasl.jaas.config"));
        assertEquals(P + in.get("sasl.login.callback.handler.class"), out.get("sasl.login.callback.handler.class"));
        assertEquals(P + in.get("value.subject.name.strategy"), out.get("value.subject.name.strategy"));
        assertEquals(P + in.get("value.schema.id.deserializer"), out.get("value.schema.id.deserializer"));
        assertEquals(P + "org.apache.kafka.A,com.example.B", out.get("interceptor.classes"));
        assertEquals(P + in.get("metric.reporters"), out.get("metric.reporters"));
        assertEquals(P + in.get("config.providers.file.class"), out.get("config.providers.file.class"));
        assertEquals(in.get("bootstrap.servers"), out.get("bootstrap.servers"), "not a class setting");
        assertEquals("SASL_SSL", out.get("security.protocol"));
    }

    @Test
    void withoutAPrefixNothingChanges() {
        Map<String, String> in = Map.of("sasl.jaas.config", PLAIN + " required;", "metric.reporters", "org.apache.kafka.X");
        assertEquals(in, ShadedNames.rewrite(in, ""));
        assertEquals(in, ShadedNames.rewrite(in));
    }
}
