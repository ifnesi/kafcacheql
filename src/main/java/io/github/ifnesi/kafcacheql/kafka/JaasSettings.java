package io.github.ifnesi.kafcacheql.kafka;

import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.Secrets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;

/**
 * {@code sasl.jaas.config}, read with Kafka's own parser before any client exists. Kafka's parse errors quote
 * the text they reject, which can be part of a credential, so the driver reports none of it. In the shaded
 * JAR, a login module named by its original Kafka or Confluent class is rewritten to its relocated class:
 * the setting is rebuilt from what Kafka parsed, so comments and quoting can't hide the class name.
 */
final class JaasSettings {

    static final String KEY = SaslConfigs.SASL_JAAS_CONFIG;

    private JaasSettings() {}

    /** The value to give the clients: unchanged, or rebuilt with relocated login modules. */
    static String prepare(String value, String prefix) throws SQLException {
        List<AppConfigurationEntry> entries = parse(value);
        if (prefix.isEmpty() || entries.stream().noneMatch(e -> ShadedNames.relocatable(e.getLoginModuleName()))) {
            return value;
        }
        return serialize(entries, prefix);
    }

    static List<AppConfigurationEntry> parse(String value) throws SQLException {
        try {
            return JaasContext.loadClientContext(Map.of(KEY, new Password(value))).configurationEntries();
        } catch (RuntimeException e) {
            throw ConfigErrors.problem("sasl.jaas.config can't be parsed. It takes one login module: <class> "
                    + "<required|requisite|sufficient|optional> option=\"value\" ...; with option values in quotes "
                    + "when they hold spaces or symbols");
        }
    }

    /** Login modules as Kafka's parser reads them back: keys and values always double-quoted and escaped. */
    static String serialize(List<AppConfigurationEntry> entries, String prefix) {
        StringBuilder out = new StringBuilder();
        for (AppConfigurationEntry e : entries) {
            if (!out.isEmpty()) {
                out.append(' ');
            }
            String module = e.getLoginModuleName();
            out.append(ShadedNames.relocatable(module) ? prefix + module : module)
                    .append(' ').append(flag(e.getControlFlag()));
            // Keys too: Kafka accepts quoted keys, such as "ignored option", which written bare would split.
            for (Map.Entry<String, ?> option : new TreeMap<>(e.getOptions()).entrySet()) {
                out.append(" \"").append(escape(option.getKey())).append("\"=\"")
                        .append(escape(String.valueOf(option.getValue()))).append('"');
            }
            out.append(';');
        }
        return out.toString();
    }

    private static String flag(LoginModuleControlFlag flag) {
        if (flag == LoginModuleControlFlag.REQUIRED) {
            return "required";
        }
        if (flag == LoginModuleControlFlag.REQUISITE) {
            return "requisite";
        }
        if (flag == LoginModuleControlFlag.SUFFICIENT) {
            return "sufficient";
        }
        return "optional";
    }

    /** Escapes as java.io.StreamTokenizer, Kafka's tokenizer, reads them inside double quotes. */
    static String escape(String value) {
        return Secrets.jaasEscape(value);
    }
}
