package io.github.ifnesi.jdbckafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void maskedAndRedacted() {
        Map<String, String> settings = Map.of("sasl.jaas.config", "jaas-S3CR3T", "security.protocol", "SASL_SSL",
                "basic.auth.user.info", "u:S3CR3T", "ssl.key.password", "");
        assertEquals(Map.of("sasl.jaas.config", "[hidden]", "security.protocol", "SASL_SSL",
                "basic.auth.user.info", "[hidden]", "ssl.key.password", "[hidden]"), Secrets.masked(settings));
        assertEquals("a [hidden] b [hidden] SASL_SSL",
                Secrets.redact("a jaas-S3CR3T b u:S3CR3T SASL_SSL", settings));
    }
}
