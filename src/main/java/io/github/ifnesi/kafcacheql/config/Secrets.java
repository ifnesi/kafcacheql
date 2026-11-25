package io.github.ifnesi.kafcacheql.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;

/** Credential masking for settings and client errors. Never show a setting containing '@' in toString(). */
public final class Secrets {

    public static final String HIDDEN = "[hidden]";

    private static final Set<String> PASSWORD_KEYS = DriverLoader.call(() -> Stream.of(
                    ConsumerConfig.configDef(), AdminClientConfig.configDef(), ProducerConfig.configDef(),
                    AbstractKafkaSchemaSerDeConfig.baseConfigDef())
            .flatMap(d -> d.configKeys().values().stream())
            .filter(k -> k.type == ConfigDef.Type.PASSWORD)
            .map(k -> k.name)
            .collect(Collectors.toUnmodifiableSet()));

    private static final Pattern LOOKS_SECRET = Pattern.compile(
            "(?i).*(password|passphrase|secret|jaas|user\\.info|token|credential|private\\.key|keystore\\.key).*");

    private static final String JAAS = SaslConfigs.SASL_JAAS_CONFIG;

    private static final Pattern URL_USERINFO = Pattern.compile("([A-Za-z][A-Za-z0-9+.-]*://)([^/?#]*)@");

    private Secrets() {}

    public static boolean isSecret(String key) {
        return key.startsWith("request.header.") || PASSWORD_KEYS.contains(key) || LOOKS_SECRET.matcher(key).matches();
    }

    /** A copy for toString() and logs: secret values, and credentials inside URLs, replaced by {@value #HIDDEN}. */
    public static Map<String, String> masked(Map<String, ?> settings) {
        Map<String, String> out = new TreeMap<>();
        settings.forEach((k, v) -> out.put(k, isSecret(k) || String.valueOf(v).contains("@") ? HIDDEN : String.valueOf(v)));
        return out;
    }

    public static String hideUrlUserInfo(String text) {
        return URL_USERINFO.matcher(text).replaceAll("$1" + Matcher.quoteReplacement(HIDDEN) + "@");
    }

    /** {@code text} with every credential of {@code settings}, whole or in parts, replaced by {@value #HIDDEN}. */
    public static String redact(String text, Map<String, ?> settings) {
        if (text == null) {
            return null;
        }
        Set<String> hide = new LinkedHashSet<>();
        settings.forEach((key, v) -> hide.addAll(sensitive(key, v == null ? "" : v.toString())));
        List<String> longestFirst = new ArrayList<>(hide);
        longestFirst.sort(Comparator.comparingInt(String::length).reversed());
        String out = text;
        for (String s : longestFirst) {
            out = out.replace(s, HIDDEN);
        }
        return hideUrlUserInfo(out);
    }

    /** Everything of this setting that redaction hides. */
    static Set<String> sensitive(String key, String value) {
        Set<String> out = new LinkedHashSet<>();
        if (value.isEmpty()) {
            return out;
        }
        if (isSecret(key)) {
            out.add(value);
            if (key.equals(JAAS)) {
                out.addAll(jaasValues(value));
            } else if (key.endsWith("basic.auth.user.info")) {
                out.addAll(userInfoParts(value));
            } else {
                out.addAll(words(value));
            }
        }
        Matcher m = URL_USERINFO.matcher(value);
        while (m.find()) {
            out.addAll(userInfoParts(m.group(2)));
        }
        out.remove("");
        return out;
    }

    /** The option values of a JAAS value, as Kafka parses them, with their words and escaped forms. */
    private static Set<String> jaasValues(String value) {
        Set<String> out = new LinkedHashSet<>();
        List<AppConfigurationEntry> entries;
        try {
            entries = JaasContext.loadClientContext(Map.of(JAAS, new Password(value))).configurationEntries();
        } catch (RuntimeException unparseable) {
            // Malformed JAAS is refused with a generic error before any client sees it.
            return Set.of(value);
        }
        for (AppConfigurationEntry e : entries) {
            for (Object v : e.getOptions().values()) {
                String s = String.valueOf(v);
                out.add(s);
                out.add(jaasEscape(s));
                out.addAll(words(s));
            }
        }
        return out;
    }

    /** user, password and both whole, as written and percent-decoded. */
    private static Set<String> userInfoParts(String userInfo) {
        Set<String> out = new LinkedHashSet<>();
        for (String form : List.of(userInfo, percentDecoded(userInfo))) {
            out.add(form);
            int colon = form.indexOf(':');
            if (colon >= 0) {
                out.add(form.substring(0, colon));
                out.add(form.substring(colon + 1));
            }
        }
        return out;
    }

    private static List<String> words(String value) {
        return List.of(value.trim().split("\\s+"));
    }

    private static String percentDecoded(String value) {
        try {
            return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException malformed) {
            return value;
        }
    }

    /** Escapes as java.io.StreamTokenizer, Kafka's JAAS tokenizer, reads them inside double quotes. */
    public static String jaasEscape(String value) {
        StringBuilder out = new StringBuilder();
        for (char c : value.toCharArray()) {
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '"' -> out.append("\\\"");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20 || c == 0x7f) {
                        out.append(String.format("\\%03o", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    // ------------------------------------------------------------------ exceptions

    /**
     * A copy of {@code t}'s whole chain (causes and suppressed exceptions) with every message redacted, safe
     * to attach as a cause or print. Each copy keeps its original's class name and stack trace.
     */
    public static RedactedException sanitize(Throwable t, Map<String, ?> settings) {
        return sanitize(t, settings, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static RedactedException sanitize(Throwable t, Map<String, ?> settings, Set<Throwable> seen) {
        seen.add(t);
        RedactedException copy = new RedactedException(t.getClass().getName(), redact(t.getMessage(), settings));
        copy.setStackTrace(t.getStackTrace());
        Throwable cause = t.getCause();
        if (cause != null && !seen.contains(cause)) {
            copy.initCause(sanitize(cause, settings, seen));
        }
        for (Throwable s : t.getSuppressed()) {
            if (!seen.contains(s)) {
                copy.addSuppressed(sanitize(s, settings, seen));
            }
        }
        return copy;
    }

    /** An exception copied with its message redacted; prints as its original's class name and message. */
    public static final class RedactedException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private final String originalClass;

        RedactedException(String originalClass, String message) {
            super(message);
            this.originalClass = originalClass;
        }

        public String originalClass() {
            return originalClass;
        }

        @Override
        public String toString() {
            return getMessage() == null ? originalClass : originalClass + ": " + getMessage();
        }
    }
}
