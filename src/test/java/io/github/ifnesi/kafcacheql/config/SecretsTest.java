package io.github.ifnesi.kafcacheql.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class SecretsTest {

    // PLAN §4.5 and §4.7: client credential values must never be shown.
    @ParameterizedTest
    @ValueSource(strings = {
            // typed PASSWORD by Kafka or Confluent
            "sasl.jaas.config", "ssl.key.password", "ssl.keystore.password", "ssl.truststore.password",
            "ssl.keystore.key", "sasl.oauthbearer.client.credentials.client.secret", "basic.auth.user.info",
            "bearer.auth.token", "bearer.auth.client.secret", "schema.registry.ssl.keystore.password",
            "schema.registry.basic.auth.user.info",
            // defined by neither, named like a credential
            "my.vault.password", "custom.secret", "x.api.token"})
    void secrets(String key) {
        assertTrue(Secrets.isSecret(key), key);
    }

    @ParameterizedTest
    @ValueSource(strings = {"bootstrap.servers", "security.protocol", "sasl.mechanism", "schema.registry.url",
            "session.timeout.ms", "client.dns.lookup", "topics.exclude"})
    void notSecrets(String key) {
        assertFalse(Secrets.isSecret(key), key);
    }

    /** PLAN §4.5: "secret values, their fragments". What to hide follows where text appears: a password can look like a class name, a flag or a short word. */
    @Test
    void partsAreHiddenByRoleNotByShape() {
        String plain = "org.apache.kafka.common.security.plain.PlainLoginModule";
        Map<String, String> s = Map.of("sasl.jaas.config",
                plain + " required username='alice' password='DOTTED.SECRET' token='required' other='abc';");
        assertEquals("[hidden] [hidden] [hidden]", Secrets.redact("DOTTED.SECRET required abc", s));
        assertEquals("No LoginModule found for " + plain, Secrets.redact("No LoginModule found for " + plain, s),
                "the login module is no option value");
        assertEquals("[hidden]", Secrets.redact("DOTTED.SECRET", Map.of("basic.auth.user.info", "alice:DOTTED.SECRET")));

        IllegalArgumentException inner = new IllegalArgumentException("DOTTED.SECRET");
        inner.addSuppressed(new IllegalStateException("required abc"));
        Secrets.RedactedException copy = Secrets.sanitize(new RuntimeException("outer", inner), s);
        assertEquals("java.lang.IllegalArgumentException: [hidden]", copy.getCause().toString());
        assertEquals("java.lang.IllegalStateException: [hidden] [hidden]", copy.getCause().getSuppressed()[0].toString());
    }

    /** PLAN §4.5: "Errors, and the causes they carry, are redacted"; suppressed chains need a unit probe. */
    @Test
    void sanitizedChainsKeepClassesAndStackTracesButNoCredentials() {
        Map<String, String> settings = Map.of("ssl.key.password", "SSL-CANARY");
        IllegalStateException inner = new IllegalStateException("inner SSL-CANARY");
        RuntimeException outer = new RuntimeException("outer SSL-CANARY", inner);
        outer.addSuppressed(new IllegalArgumentException("suppressed SSL-CANARY"));

        Secrets.RedactedException copy = Secrets.sanitize(outer, settings);
        java.io.StringWriter trace = new java.io.StringWriter();
        copy.printStackTrace(new java.io.PrintWriter(trace));
        assertFalse(trace.toString().contains("SSL-CANARY"), trace.toString());
        assertTrue(trace.toString().contains("java.lang.RuntimeException: outer [hidden]"), trace.toString());
        assertTrue(trace.toString().contains("Caused by: java.lang.IllegalStateException: inner [hidden]"));
        assertTrue(trace.toString().contains("Suppressed: java.lang.IllegalArgumentException: suppressed [hidden]"));
        assertEquals(outer.getStackTrace()[0], copy.getStackTrace()[0]);
        assertEquals("java.lang.IllegalStateException", ((Secrets.RedactedException) copy.getCause()).originalClass());
    }

    @Test
    void maskedAndRedacted() {
        Map<String, String> settings = Map.of("sasl.jaas.config", "jaas-S3CR3T", "security.protocol", "SASL_SSL",
                "basic.auth.user.info", "u:S3CR3T", "ssl.key.password", "");
        assertEquals(Map.of("sasl.jaas.config", "[hidden]", "security.protocol", "SASL_SSL",
                "basic.auth.user.info", "[hidden]", "ssl.key.password", "[hidden]"), Secrets.masked(settings));
        assertEquals("a [hidden] b [hidden] SASL_SSL",
                Secrets.redact("a jaas-S3CR3T b u:S3CR3T SASL_SSL", settings));
    }
}
