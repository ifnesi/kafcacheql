package io.github.ifnesi.jdbckafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class SecretsTest {

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

    @Test
    void fragmentsOfACredentialAreRedactedToo() {
        // An unquoted password with a space: Kafka's parser quotes only its second word.
        Map<String, String> settings = Map.of("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=test password=first JAAS-CANARY;");
        String redacted = Secrets.redact("Value not specified for key 'JAAS-CANARY' in JAAS config", settings);
        assertEquals("Value not specified for key '[hidden]' in JAAS config", redacted);
        assertEquals("No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule",
                Secrets.redact("No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule", settings),
                "class names and option names aren't credentials");
    }

    /** What to hide follows where text appears: a password can look like a class name, a flag or a short word. */
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

    @Test
    void urlCredentialsAreReadAsJavaReadsThem() {
        assertEquals(Map.of("schema.registry.url", "http://[hidden]@localhost:8081"),
                Secrets.masked(Map.of("schema.registry.url", "http://alice:SPACE SECRET@localhost:8081")));
        Secrets.UrlCredentials c = Secrets.urlCredentials("http://alice:SPACE SECRET@localhost:8081/x?y=@z").orElseThrow();
        assertEquals("alice:SPACE SECRET", c.userInfo());
        assertEquals("http://localhost:8081/x?y=@z", c.withoutUserInfo());
        assertFalse(c.ambiguous());
        assertTrue(Secrets.urlCredentials("http://a@b@host:1").orElseThrow().ambiguous());
        assertNull(Secrets.urlCredentials("http://host:1/p@x").orElseThrow().userInfo(), "an '@' in the path isn't credentials");
        assertTrue(Secrets.urlCredentials("not a url").isEmpty());
        for (String prefixed : List.of("url:http://alice:PREFIX@h:1/p", " URL:HTTP://alice:PREFIX@h:1/p")) {
            Secrets.UrlCredentials p = Secrets.urlCredentials(prefixed).orElseThrow();
            assertEquals("alice:PREFIX", p.userInfo(), prefixed);
            assertEquals("http://h:1/p", p.withoutUserInfo(), prefixed);
            assertFalse(p.toString().contains("PREFIX"), p.toString());
        }
        assertFalse(Secrets.urlCredentials("http://a@b:SECRET@h").orElseThrow().toString().contains("SECRET"));
        String malformed = "http://alice:HEAD@TAIL/SLASH-CANARY@r:8081";
        assertEquals(Map.of("schema.registry.url", "http://[hidden]@r:8081"), Secrets.masked(Map.of("schema.registry.url", malformed)));
        assertFalse(Secrets.urlCredentials(malformed).orElseThrow().toString().contains("CANARY"));
        assertEquals("[hidden] [hidden]", Secrets.redact("TAIL/SLASH-CANARY HEAD", Map.of("schema.registry.url", malformed)));
        assertEquals(Map.of("u", "http://[hidden]@r1,http://[hidden]@r2"), Secrets.masked(Map.of("u", "http://a:x@r1,http://b:y@r2")),
                "each URL of a list on its own");
        // A raw ',' in credentials: the value can't be split safely, so all of it up to the last '@' is hidden.
        String comma = "http://alice:FIRST,COMMA-CANARY@localhost:8081";
        assertEquals(Map.of("u", "http://[hidden]@localhost:8081"), Secrets.masked(Map.of("u", comma)));
        assertEquals("[hidden] [hidden] [hidden]", Secrets.redact("FIRST COMMA-CANARY FIRST,COMMA-CANARY", Map.of("u", comma)));
        assertEquals(Map.of("u", "http://[hidden]@host"), Secrets.masked(Map.of("u", "http://alice:1234,http://x@host")));
        assertEquals(Map.of("u", "http://[hidden]@h"), Secrets.masked(Map.of("u", "http://a:FIRST%2CCANARY@h")), "an encoded ',' is no separator");
        assertEquals(Map.of("u", "broker-1:9092,broker-2:9092"), Secrets.masked(Map.of("u", "broker-1:9092,broker-2:9092")));
        Map<String, String> encoded = Map.of("schema.registry.url", "http://user:pa%40ss@h");
        assertEquals("ping http://[hidden]@h failed", Secrets.redact("ping http://user:pa%40ss@h failed", encoded));
        assertEquals("[hidden]", Secrets.redact("pa@ss", encoded), "the decoded form too");
    }

    @Test
    void credentialsInUrlsAreHidden() {
        Map<String, String> settings = Map.of("schema.registry.url", "http://user:URL-CANARY@registry:8081");
        assertEquals(Map.of("schema.registry.url", "http://[hidden]@registry:8081"), Secrets.masked(settings));
        assertEquals("connecting to http://[hidden]@registry:8081 as [hidden] failed",
                Secrets.redact("connecting to http://user:URL-CANARY@registry:8081 as URL-CANARY failed", settings));
        assertEquals("see https://[hidden]@x/y", Secrets.hideUrlUserInfo("see https://a:b@x/y"));
    }

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
