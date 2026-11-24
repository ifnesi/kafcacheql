package io.github.ifnesi.kafcacheql.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

/**
 * Which client settings hold credentials, and how to keep them out of anything the driver shows: values
 * Kafka's and Confluent's own definitions type as PASSWORD, settings neither defines whose name looks like a
 * credential, and credentials written into URLs ({@code scheme://user:password@host}).
 *
 * <p>What to hide is decided by where text appears, never by what it looks like: a parser that rejects a
 * credential often quotes only part of it, and a password can look like a class name or a keyword. So the
 * hidden set holds each secret value whole, its parts by role (the option values of sasl.jaas.config as
 * Kafka parses them, both halves of user:password, the words of other secret values) and their escaped and
 * decoded forms.
 */
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
    private static final Set<String> JAAS_FLAGS = Set.of("required", "requisite", "sufficient", "optional");

    /**
     * The credentials of a URL: {@code scheme://<this>@host}, wherever the URL starts in the text (after a
     * "url:" prefix too). The span runs to the last '@' before the path, query or fragment: java.net.URL reads
     * the part before a single '@' as credentials, and masking hides through the last one whatever their
     * number.
     */
    private static final Pattern URL_USERINFO = Pattern.compile("([A-Za-z][A-Za-z0-9+.-]*://)([^/?#]*)@");

    /** Each URL of a list that splits safely (see hideUrlCredentials): from its "://" to its last '@'. */
    private static final Pattern URL_ENTRY_CREDENTIALS = Pattern.compile("([A-Za-z][A-Za-z0-9+.-]*://)([^,]*)@");

    /** A whole value: from its first "://" to its last '@', commas and all. */
    private static final Pattern VALUE_CREDENTIALS = Pattern.compile("([A-Za-z][A-Za-z0-9+.-]*://)(.*)@", Pattern.DOTALL);

    private Secrets() {}

    public static boolean isSecret(String key) {
        return PASSWORD_KEYS.contains(key) || LOOKS_SECRET.matcher(key).matches();
    }

    /** A copy for toString() and logs: secret values, and credentials inside URLs, replaced by {@value #HIDDEN}. */
    public static Map<String, String> masked(Map<String, ?> settings) {
        Map<String, String> out = new TreeMap<>();
        settings.forEach((k, v) -> out.put(k, isSecret(k) ? HIDDEN : hideUrlCredentials(String.valueOf(v))));
        return out;
    }

    /**
     * A setting's value as shown, conservatively: beyond java.net.URL's credentials, it hides malformed
     * attempts at them (an unencoded '/' or ',' in a password), which the driver refuses but shows first, in
     * toString(). A value splits safely at its commas only when every URL in it has exactly one '@', ending
     * its credentials, as ClientSettings accepts a list: then each URL's credentials are hidden on their own,
     * leaving the hosts. Otherwise everything from the first "://" to the last '@' is hidden.
     */
    public static String hideUrlCredentials(String value) {
        if (value.indexOf('@') < 0) {
            return value;
        }
        return credentialSpans(value).isEmpty() ? value : (splitsSafely(value)
                ? URL_ENTRY_CREDENTIALS.matcher(value).replaceAll("$1" + Matcher.quoteReplacement(HIDDEN) + "@")
                : VALUE_CREDENTIALS.matcher(value).replaceFirst("$1" + Matcher.quoteReplacement(HIDDEN) + "@"));
    }

    /** What hideUrlCredentials hides. */
    private static List<String> credentialSpans(String value) {
        List<String> spans = new ArrayList<>();
        Matcher m = (splitsSafely(value) ? URL_ENTRY_CREDENTIALS : VALUE_CREDENTIALS).matcher(value);
        while (m.find()) {
            spans.add(m.group(2));
        }
        return spans;
    }

    private static boolean splitsSafely(String value) {
        if (value.indexOf(',') < 0) {
            return true;
        }
        for (String entry : value.split(",", -1)) {
            String url = entry.trim();
            if (url.isEmpty()) {
                continue;
            }
            UrlCredentials parsed = urlCredentials(url).orElse(null);
            if (parsed == null || parsed.userInfo() == null || parsed.withoutUserInfo().indexOf('@') >= 0) {
                return false;
            }
        }
        return true;
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
        List<String> spans = new ArrayList<>();
        Matcher m = URL_USERINFO.matcher(value);
        while (m.find()) {
            spans.add(m.group(2));
        }
        if (value.indexOf('@') >= 0) {
            spans.addAll(credentialSpans(value));
        }
        boolean split = splitsSafely(value);
        for (String span : spans) {
            out.add(span);
            out.addAll(userInfoParts(span));
            // Malformed ones: their pieces too, since a parser quoting one may cut it anywhere.
            for (String piece : span.split(split ? "@" : "[@,]")) {
                out.addAll(userInfoParts(piece.trim()));
            }
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
            // Nothing tells option values from the rest: hide every token but the module class and the flag.
            String[] tokens = value.trim().split("[\\s;=\"']+");
            for (int i = 1; i < tokens.length; i++) {
                if (!(i == 1 && JAAS_FLAGS.contains(tokens[i]))) {
                    out.add(tokens[i]);
                }
            }
            return out;
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

    private static String percentDecoded(String s) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length() && Character.digit(s.charAt(i + 1), 16) >= 0
                    && Character.digit(s.charAt(i + 2), 16) >= 0) {
                bytes.write(Character.digit(s.charAt(i + 1), 16) * 16 + Character.digit(s.charAt(i + 2), 16));
                i += 2;
            } else {
                byte[] b = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                bytes.write(b, 0, b.length);
            }
        }
        return bytes.toString(StandardCharsets.UTF_8);
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

    // ------------------------------------------------------------------ credentials in URLs

    /**
     * A URL split around its credentials, as java.net.URL reads it, the parser the Schema Registry client
     * uses: the text trimmed, an optional "url:" prefix in any case, and credentials before a single '@' in
     * the authority, which ends at the first '/' or '?'.
     *
     * @param withoutUserInfo the URL without its credentials (and without a "url:" prefix)
     * @param userInfo the credentials, or null when the URL has none
     * @param ambiguous more than one '@' in the authority: java.net.URL then reads neither credentials nor host
     */
    public record UrlCredentials(String withoutUserInfo, String userInfo, boolean ambiguous) {

        @Override
        public String toString() {
            return "UrlCredentials[withoutUserInfo=" + hideUrlCredentials(withoutUserInfo) + ", userInfo="
                    + (userInfo == null ? null : HIDDEN) + ", ambiguous=" + ambiguous + "]";
        }
    }

    /** Empty when java.net.URL doesn't accept {@code url}. */
    @SuppressWarnings("deprecation") // new URL(String) is how the Schema Registry client reads them
    public static Optional<UrlCredentials> urlCredentials(String url) {
        java.net.URL parsed;
        try {
            parsed = new java.net.URL(url);
        } catch (java.net.MalformedURLException e) {
            return Optional.empty();
        }
        String authority = parsed.getAuthority();
        int at = authority == null ? -1 : authority.indexOf('@');
        if (at < 0) {
            return Optional.of(new UrlCredentials(url, null, false));
        }
        if (parsed.getUserInfo() == null) {
            return Optional.of(new UrlCredentials(url, null, true));
        }
        String ref = parsed.getRef() == null ? "" : "#" + parsed.getRef();
        return Optional.of(new UrlCredentials(parsed.getProtocol() + "://" + authority.substring(at + 1)
                + parsed.getFile() + ref, parsed.getUserInfo(), false));
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
