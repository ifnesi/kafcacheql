package io.github.ifnesi.kafcacheql.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** sasl.jaas.config, read with Kafka's own parser. Run unshaded, so the prefix is passed in. */
class JaasSettingsTest {

    private static final String P = "io.github.ifnesi.kafcacheql.shaded.";
    private static final String PLAIN = "org.apache.kafka.common.security.plain.PlainLoginModule";

    @Test
    void parseErrorsQuoteNothingOfTheValue() {
        // Kafka's own message would be "Value not specified for key 'JAAS-CANARY' in JAAS config".
        SQLException e = assertThrows(SQLException.class, () -> JaasSettings.prepare(
                PLAIN + " required username=test password=first JAAS-CANARY;", P));
        StringWriter trace = new StringWriter();
        e.printStackTrace(new PrintWriter(trace));
        assertFalse(trace.toString().contains("JAAS-CANARY"), trace.toString());
        assertFalse(trace.toString().contains("first"), trace.toString());
        assertTrue(e.getMessage().startsWith("sasl.jaas.config can't be parsed"), e.getMessage());
        assertEquals("08001", e.getSQLState());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", PLAIN, PLAIN + " mandatory;", PLAIN + " required username=;",
            PLAIN + " required; " + PLAIN + " required;"})
    void malformedValuesFail(String value) {
        assertThrows(SQLException.class, () -> JaasSettings.prepare(value, P));
    }

    @Test
    void unshadedTheValueIsLeftAsWritten() throws SQLException {
        String value = "/* a comment */ " + PLAIN + " required username='k' password='s';";
        assertSame(value, JaasSettings.prepare(value, ""));
    }

    @Test
    void anotherLoginModuleIsLeftAsWritten() throws SQLException {
        String krb = "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true;";
        assertSame(krb, JaasSettings.prepare(krb, P));
    }

    @Test
    void commentsCantHideTheLoginModuleFromTheRewrite() throws SQLException {
        for (String value : List.of(
                "/* valid JAAS comment */ " + PLAIN + " required username='test' password='test-secret';",
                "// a line comment\n" + PLAIN + " required username=\"test\" password=\"test-secret\";",
                "/* " + PLAIN + " in a comment */ " + PLAIN + " required username='test' password='test-secret';")) {
            AppConfigurationEntry e = only(JaasSettings.parse(JaasSettings.prepare(value, P)));
            assertEquals(P + PLAIN, e.getLoginModuleName(), value);
            assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, e.getControlFlag());
            assertEquals(Map.of("username", "test", "password", "test-secret"), e.getOptions());
        }
    }

    /** The rewritten value parses, in Kafka's parser, to the same options as the original. */
    @Test
    void optionValuesSurviveTheRewriteExactly() throws SQLException {
        String value = PLAIN + " requisite username=\"k\" password=\"a \\\\b \\\"q\\\" ;= 'x' é😀\\ttab\\nline\"" +
                " token='single \"quoted\"';";
        AppConfigurationEntry original = only(JaasSettings.parse(value));
        AppConfigurationEntry rewritten = only(JaasSettings.parse(JaasSettings.prepare(value, P)));
        assertEquals(P + PLAIN, rewritten.getLoginModuleName());
        assertEquals(original.getControlFlag(), rewritten.getControlFlag());
        assertEquals(original.getOptions(), rewritten.getOptions());
        assertEquals("a \\b \"q\" ;= 'x' é😀\ttab\nline", original.getOptions().get("password"));
    }

    /** Kafka accepts quoted option keys; written bare after the rewrite, one with a space would split. */
    @Test
    void quotedOptionKeysSurviveTheRewrite() throws SQLException {
        String value = PLAIN + " required username='test' password='test-secret' \"ignored option\"='x' \"we\\\"ird\"=\"y\";";
        AppConfigurationEntry original = only(JaasSettings.parse(value));
        AppConfigurationEntry rewritten = only(JaasSettings.parse(JaasSettings.prepare(value, P)));
        assertTrue(original.getOptions().containsKey("ignored option"), original.getOptions().toString());
        assertTrue(original.getOptions().containsKey("we\"ird"), original.getOptions().toString());
        assertEquals(original.getOptions(), rewritten.getOptions());
    }

    @Test
    void controlCharactersAreEscapedAsTheTokenizerReadsThem() throws SQLException {
        String serialized = JaasSettings.serialize(JaasSettings.parse(PLAIN + " optional a=\"x\";"), P);
        assertEquals(P + PLAIN + " optional \"a\"=\"x\";", serialized);
        assertEquals("\\001\\\\\\\"", JaasSettings.escape("\u0001\\\""));
    }

    private static AppConfigurationEntry only(List<AppConfigurationEntry> entries) {
        assertEquals(1, entries.size());
        return entries.get(0);
    }
}
