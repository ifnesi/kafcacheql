package io.github.ifnesi.kafcacheql.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * The connection's configuration: layers merged in the order built-in defaults,
 * {@code config.file}, URL parameters, {@code Properties} object, later layers winning.
 *
 * @param bootstrapServers from the URL, or from {@code bootstrap.servers}; empty only when offline
 * @param cachePath empty: {@code ~/.kafcacheql/<cluster-id>.db}, resolved once the cluster ID is known
 * @param clientSettings every property that isn't the driver's own, for the clients, with the
 *     effective {@code bootstrap.servers}
 */
public record DriverConfig(
        Optional<String> bootstrapServers,
        Optional<Path> cachePath,
        OptionalLong cacheMaxBytes,
        OptionalLong cacheRetentionHours,
        Optional<String> maskSecret,
        Window appendDefaultWindow,
        long appendDefaultWindowMaxRecords,
        long maxStalenessMs,
        boolean offline,
        Pattern topicsInclude,
        Pattern topicsExclude,
        boolean metadataShowInternal,
        String clientId,
        int syncBatchRecords,
        int syncFetchParallelism,
        Map<String, TopicSettings> topics,
        Map<String, String> clientSettings) {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String TOPIC_PREFIX = "topic.";
    /** Kafka's rule: 1 to 249 of A-Z, a-z, 0-9, '.', '_' and '-', but not "." or "..". */
    private static final Pattern TOPIC_NAME = Pattern.compile("(?!\\.{1,2}$)[A-Za-z0-9._-]{1,249}");
    private static final Pattern DURATION = Pattern.compile("([0-9]{1,18})(ms|s|m|h|d)");

    /** The topic's overrides; {@link TopicSettings#NONE} when it has none. */
    public TopicSettings topic(String name) {
        return topics.getOrDefault(name, TopicSettings.NONE);
    }

    /** The default window that applies to the topic: its override, or the connection's. */
    public Window appendDefaultWindow(String topic) {
        return topic(topic).appendDefaultWindow().orElse(appendDefaultWindow);
    }

    public long appendDefaultWindowMaxRecords(String topic) {
        return topic(topic).appendDefaultWindowMaxRecords().orElse(appendDefaultWindowMaxRecords);
    }

    /** Client settings' credentials are hidden; the record's own toString would print them. */
    @Override
    public String toString() {
        return "DriverConfig[bootstrapServers=" + bootstrapServers + ", cachePath=" + cachePath
                + ", cacheMaxBytes=" + cacheMaxBytes + ", cacheRetentionHours=" + cacheRetentionHours
                + ", appendDefaultWindow=" + appendDefaultWindow
                + ", appendDefaultWindowMaxRecords=" + appendDefaultWindowMaxRecords
                + ", maxStalenessMs=" + maxStalenessMs
                + ", offline=" + offline
                + ", topicsInclude=" + topicsInclude + ", topicsExclude=" + topicsExclude
                + ", metadataShowInternal=" + metadataShowInternal + ", clientId=" + clientId
                + ", syncBatchRecords=" + syncBatchRecords + ", syncFetchParallelism=" + syncFetchParallelism + ", topics=" + topics
                + ", clientSettings=" + Secrets.masked(clientSettings) + "]";
    }

    /** All secrets for exception redaction, including driver-only settings never passed to clients. */
    public Map<String, String> redactions() {
        var all = new TreeMap<>(clientSettings);
        maskSecret.ifPresent(secret -> all.put("mask.secret", secret));
        return all;
    }

    // ------------------------------------------------------------------ loading

    public static DriverConfig load(String url, Properties info) throws SQLException {
        JdbcUrl jdbcUrl = JdbcUrl.parse(url);
        Map<String, String> given = strings(info);
        String configFileKey = DriverProperty.CONFIG_FILE.key();
        String configFile = given.getOrDefault(configFileKey, jdbcUrl.params().get(configFileKey));

        Map<String, String> merged = new LinkedHashMap<>();
        for (DriverProperty p : DriverProperty.values()) {
            if (p.defaultValue() != null) {
                merged.put(p.key(), p.defaultValue());
            }
        }
        if (configFile != null) {
            merged.putAll(readConfigFile(path(configFileKey, configFile)));
        }
        merged.putAll(jdbcUrl.params());
        merged.putAll(given);
        merged.remove(configFileKey);
        return build(jdbcUrl, merged);
    }

    private static DriverConfig build(JdbcUrl url, Map<String, String> merged) throws SQLException {
        Map<DriverProperty, String> own = new LinkedHashMap<>();
        Map<String, TopicSettings> topics = new TreeMap<>();
        Map<String, String> client = new TreeMap<>();
        for (Map.Entry<String, String> e : merged.entrySet()) {
            Optional<DriverProperty> p = DriverProperty.byKey(e.getKey());
            if (p.isPresent()) {
                own.put(p.get(), e.getValue());
            } else if (e.getKey().startsWith(TOPIC_PREFIX)) {
                topicSetting(topics, e.getKey(), e.getValue());
            } else {
                client.put(e.getKey(), e.getValue());
            }
        }

        boolean offline = bool(own, DriverProperty.OFFLINE);
        String cachePathValue = own.get(DriverProperty.CACHE_PATH);
        Optional<Path> cachePath = cachePathValue == null
                ? Optional.empty() : Optional.of(path(DriverProperty.CACHE_PATH.key(), cachePathValue));
        if (offline && cachePath.isEmpty()) {
            throw ConfigErrors.problem("offline=true needs cache.path: the cluster ID that names the default cache "
                    + "file can't be looked up offline");
        }

        Optional<String> bootstrap = bootstrap(url, client.get(BOOTSTRAP_SERVERS));
        if (bootstrap.isEmpty() && !offline) {
            throw ConfigErrors.problem("No bootstrap servers: put them in the URL (jdbc:kafcacheql://host:9092) "
                    + "or set bootstrap.servers");
        }
        bootstrap.ifPresent(b -> client.put(BOOTSTRAP_SERVERS, b));

        String clientId = own.get(DriverProperty.CLIENT_ID);
        if (clientId.isBlank()) {
            throw ConfigErrors.invalid(DriverProperty.CLIENT_ID.key(), clientId, "a non-empty name");
        }

        if (topics.values().stream().anyMatch(t -> t.transforms().enabled())
                && (own.get(DriverProperty.MASK_SECRET) == null || own.get(DriverProperty.MASK_SECRET).isEmpty()))
            throw ConfigErrors.problem("mask.secret is required when any topic has a record transform");

        return new DriverConfig(
                bootstrap,
                cachePath,
                own.containsKey(DriverProperty.CACHE_MAX_BYTES) ? OptionalLong.of(atLeast(DriverProperty.CACHE_MAX_BYTES.key(),
                        own.get(DriverProperty.CACHE_MAX_BYTES), 1, Long.MAX_VALUE)) : OptionalLong.empty(),
                own.containsKey(DriverProperty.CACHE_RETENTION_HOURS) ? OptionalLong.of(atLeast(DriverProperty.CACHE_RETENTION_HOURS.key(),
                        own.get(DriverProperty.CACHE_RETENTION_HOURS), 1, Long.MAX_VALUE / Duration.ofHours(1).toMillis())) : OptionalLong.empty(),
                Optional.ofNullable(own.get(DriverProperty.MASK_SECRET)),
                window(DriverProperty.APPEND_DEFAULT_WINDOW.key(), own.get(DriverProperty.APPEND_DEFAULT_WINDOW)),
                atLeast(DriverProperty.APPEND_DEFAULT_WINDOW_MAX_RECORDS.key(),
                        own.get(DriverProperty.APPEND_DEFAULT_WINDOW_MAX_RECORDS), 1, Long.MAX_VALUE),
                atLeast(DriverProperty.MAX_STALENESS_MS.key(), own.get(DriverProperty.MAX_STALENESS_MS), 0, Long.MAX_VALUE),
                offline,
                regex(own, DriverProperty.TOPICS_INCLUDE),
                regex(own, DriverProperty.TOPICS_EXCLUDE),
                bool(own, DriverProperty.METADATA_SHOW_INTERNAL),
                clientId,
                (int) atLeast(DriverProperty.SYNC_BATCH_RECORDS.key(), own.get(DriverProperty.SYNC_BATCH_RECORDS), 1, Integer.MAX_VALUE),
                (int) atLeast(DriverProperty.SYNC_FETCH_PARALLELISM.key(), own.get(DriverProperty.SYNC_FETCH_PARALLELISM), 1, Integer.MAX_VALUE),
                Collections.unmodifiableMap(topics),
                Collections.unmodifiableMap(client));
    }

    /** URL hosts win; a different bootstrap.servers elsewhere is a conflict, not silently ignored. */
    private static Optional<String> bootstrap(JdbcUrl url, String setting) throws SQLException {
        if (setting != null && !setting.isBlank()) JdbcUrl.validateBootstrapServers(setting);
        if (url.servers().isEmpty()) {
            return Optional.ofNullable(setting).map(String::trim).filter(s -> !s.isEmpty());
        }
        String fromUrl = String.join(",", url.servers());
        if (setting != null && !serverSet(setting).equals(serverSet(fromUrl))) {
            throw ConfigErrors.problem("The URL's hosts and bootstrap.servers disagree; set the servers in one place");
        }
        return Optional.of(fromUrl);
    }

    private static Set<String> serverSet(String servers) {
        return Arrays.stream(servers.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

    /**
     * {@code topic.<name>.<setting>}. No setting's suffix ends with another's (DriverConfigTest checks), so a
     * key ends with at most one of them and a topic name may itself end with a setting's suffix.
     */
    private static void topicSetting(Map<String, TopicSettings> topics, String key, String value) throws SQLException {
        for (TopicSettings.Setting setting : TopicSettings.Setting.values()) {
            String suffix = "." + setting.suffix;
            if (key.endsWith(suffix) && key.length() >= TOPIC_PREFIX.length() + suffix.length()) {
                String topic = key.substring(TOPIC_PREFIX.length(), key.length() - suffix.length());
                if (!TOPIC_NAME.matcher(topic).matches()) {
                    throw ConfigErrors.problem("Invalid topic name in " + key
                            + ": topic names are 1 to 249 of A-Z, a-z, 0-9, '.', '_' and '-', and not . or ..");
                }
                Object parsed = switch (setting) {
                    case MODE -> Mode.parse(value).orElseThrow(
                            () -> ConfigErrors.invalid(key, value, String.join(" or ", Mode.choices())));
                    case KEY_FORMAT, VALUE_FORMAT -> Format.parse(value).orElseThrow(
                            () -> ConfigErrors.invalid(key, value, "one of " + String.join(", ", Format.choices())));
                    case APPEND_DEFAULT_WINDOW -> window(key, value);
                    case APPEND_DEFAULT_WINDOW_MAX_RECORDS -> atLeast(key, value, 1, Long.MAX_VALUE);
                    case DELETE_RETENTION_MS -> atLeast(key, value, 0, Long.MAX_VALUE);
                    case DROP, MASK -> TransformSettings.list(key, value, true);
                    case DROP_HEADERS, MASK_HEADERS -> TransformSettings.list(key, value, false);
                    case TRANSFORM -> value.trim();
                };
                topics.put(topic, topics.getOrDefault(topic, TopicSettings.NONE).with(setting, parsed));
                return;
            }
        }
        throw ConfigErrors.problem("Unknown per-topic setting " + key + ": topic.<name>.<setting> takes "
                + Arrays.stream(TopicSettings.Setting.values()).map(s -> s.suffix).collect(Collectors.joining(", ")));
    }

    // ------------------------------------------------------------------ values

    static Window window(String key, String value) throws SQLException {
        if ("all".equals(value)) {
            return Window.ALL;
        }
        return Window.of(duration(key, value, "a positive duration such as 500ms, 90s, 15m, 1h or 2d, or all"));
    }

    static Duration duration(String key, String value, String expected) throws SQLException {
        Matcher m = DURATION.matcher(value);
        if (!m.matches()) {
            throw ConfigErrors.invalid(key, value, expected);
        }
        long amount = Long.parseLong(m.group(1));
        ChronoUnit unit = switch (m.group(2)) {
            case "ms" -> ChronoUnit.MILLIS;
            case "s" -> ChronoUnit.SECONDS;
            case "m" -> ChronoUnit.MINUTES;
            case "h" -> ChronoUnit.HOURS;
            default -> ChronoUnit.DAYS;
        };
        try {
            Duration d = Duration.of(amount, unit);
            d.toMillis(); // must fit in milliseconds
            if (d.isZero()) {
                throw ConfigErrors.invalid(key, value, expected);
            }
            return d;
        } catch (ArithmeticException e) {
            throw ConfigErrors.invalid(key, value, expected);
        }
    }

    static long atLeast(String key, String value, long min, long max) throws SQLException {
        String expected = min == 0 ? "a whole number, 0 or more" : "a whole number, " + min + " or more";
        if (max == Integer.MAX_VALUE) {
            expected += ", at most " + max;
        }
        try {
            long n = Long.parseLong(value.trim());
            if (n < min || n > max) {
                throw ConfigErrors.invalid(key, value, expected);
            }
            return n;
        } catch (NumberFormatException e) {
            throw ConfigErrors.invalid(key, value, expected);
        }
    }

    private static boolean bool(Map<DriverProperty, String> own, DriverProperty p) throws SQLException {
        String value = own.get(p);
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw ConfigErrors.invalid(p.key(), value, "true or false");
    }

    private static Pattern regex(Map<DriverProperty, String> own, DriverProperty p) throws SQLException {
        String value = own.get(p);
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw ConfigErrors.invalid(p.key(), value, "a Java regular expression: " + e.getDescription());
        }
    }

    /** {@code ~} and {@code ~/...} start at the user's home directory. */
    static Path path(String key, String value) throws SQLException {
        String home = System.getProperty("user.home");
        try {
            if (value.equals("~")) {
                return Path.of(home);
            }
            if (value.startsWith("~/")) {
                return Path.of(home, value.substring(2));
            }
            return Path.of(value);
        } catch (InvalidPathException e) {
            throw ConfigErrors.problem("Invalid value for " + key + ": not a file path (" + e.getReason() + ")");
        }
    }

    private static Map<String, String> readConfigFile(Path file) throws SQLException {
        Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            props.load(reader);
        } catch (IOException e) {
            throw ConfigErrors.problem("config.file " + ConfigErrors.detail(e), e);
        } catch (IllegalArgumentException e) {
            // Properties.load: a malformed unicode escape. Its message quotes nothing from the file.
            throw ConfigErrors.problem("config.file " + file + " isn't a valid properties file: " + e.getMessage());
        }
        Map<String, String> values = strings(props);
        if (values.containsKey(DriverProperty.CONFIG_FILE.key())) {
            throw ConfigErrors.problem("config.file " + file + " sets config.file; it can only be set in the URL "
                    + "or the Properties object");
        }
        return values;
    }

    private static Map<String, String> strings(Properties info) {
        Map<String, String> out = new LinkedHashMap<>();
        if (info == null) {
            return out;
        }
        for (String name : info.stringPropertyNames()) {
            out.put(name, info.getProperty(name));
        }
        for (Map.Entry<Object, Object> e : info.entrySet()) {
            if (e.getKey() != null && e.getValue() != null && !(e.getValue() instanceof String)) {
                out.put(e.getKey().toString(), e.getValue().toString());
            }
        }
        return out;
    }
}
