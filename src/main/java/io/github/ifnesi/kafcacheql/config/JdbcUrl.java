package io.github.ifnesi.kafcacheql.config;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code jdbc:kafcacheql://[host:port[,host:port...]][/][?key=value&key=value...]}.
 * Parameter keys and values are percent-decoded as UTF-8; '+' stays '+', so regexes survive.
 */
public record JdbcUrl(List<String> servers, Map<String, String> params) {

    public static final String PREFIX = "jdbc:kafcacheql:";
    static final String FORM = "jdbc:kafcacheql://host:port[,host:port...][?key=value&...]";

    /** Parameter values that hold credentials are hidden; the record's own toString would print them. */
    @Override
    public String toString() {
        return "JdbcUrl[servers=" + servers + ", params=" + Secrets.masked(params) + "]";
    }

    public static boolean accepts(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    public static JdbcUrl parse(String url) throws SQLException {
        if (!accepts(url)) {
            throw ConfigErrors.malformedUrl("it must start with " + PREFIX + "//");
        }
        String rest = url.substring(PREFIX.length());
        if (!rest.startsWith("//")) {
            throw ConfigErrors.malformedUrl("the host list must follow " + PREFIX + "//");
        }
        rest = rest.substring(2);
        int q = rest.indexOf('?');
        String authority = q < 0 ? rest : rest.substring(0, q);
        String query = q < 0 ? "" : rest.substring(q + 1);
        if (authority.endsWith("/")) {
            authority = authority.substring(0, authority.length() - 1);
        }
        if (authority.contains("/")) {
            throw ConfigErrors.malformedUrl("nothing may follow the host list but ?parameters");
        }

        List<String> servers = new ArrayList<>();
        if (!authority.isEmpty()) {
            for (String server : authority.split(",", -1)) {
                servers.add(server(server));
            }
        }

        Map<String, String> params = new LinkedHashMap<>();
        for (String pair : query.split("&", -1)) {
            if (pair.isEmpty()) {
                continue; // "?a=1&" or "?&a=1"
            }
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw ConfigErrors.malformedUrl("each parameter must be key=value");
            }
            String key = decode(pair.substring(0, eq));
            String value = decode(pair.substring(eq + 1));
            if (params.putIfAbsent(key, value) != null) {
                throw ConfigErrors.malformedUrl("parameter " + key + " appears more than once");
            }
        }
        return new JdbcUrl(List.copyOf(servers), Collections.unmodifiableMap(params));
    }

    /** host:port, [ipv6]:port; hosts are letters, digits, '.', '-' and '_'. */
    private static String server(String server) throws SQLException {
        if (server.contains("@")) {
            throw ConfigErrors.malformedUrl("the host list can't hold credentials (user:password@host); "
                    + "put them in sasl.jaas.config");
        }
        String host;
        String port;
        if (server.startsWith("[")) {
            int close = server.indexOf("]:");
            if (close < 0) {
                throw ConfigErrors.malformedUrl("host " + shown(server) + " needs a port: [address]:port");
            }
            host = server.substring(1, close);
            port = server.substring(close + 2);
            if (host.isEmpty() || !host.matches("[0-9A-Fa-f:.]+")) {
                throw ConfigErrors.malformedUrl("host " + shown(server) + " isn't an IPv6 address");
            }
        } else {
            int colon = server.lastIndexOf(':');
            if (colon < 0) {
                throw ConfigErrors.malformedUrl(server.isEmpty()
                        ? "the host list has an empty entry"
                        : "host " + shown(server) + " needs a port: host:port");
            }
            host = server.substring(0, colon);
            port = server.substring(colon + 1);
            if (host.isEmpty() || !host.matches("[A-Za-z0-9._-]+")) {
                throw ConfigErrors.malformedUrl("host " + shown(server) + " isn't a host name or address");
            }
        }
        if (!port.matches("[0-9]{1,5}") || Integer.parseInt(port) < 1 || Integer.parseInt(port) > 65535) {
            throw ConfigErrors.malformedUrl("host " + shown(server) + " needs a port between 1 and 65535");
        }
        return server;
    }

    /** A host entry as errors show it: quoted when made of host characters only, else not at all. */
    private static String shown(String server) {
        return server.matches("[A-Za-z0-9._:\\[\\]-]{1,255}") ? "'" + server + "'" : "(not shown)";
    }

    private static String decode(String s) throws SQLException {
        if (s.indexOf('%') < 0) {
            return s;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int i = 0;
        while (i < s.length()) {
            int cp = s.codePointAt(i);
            if (cp == '%') {
                if (i + 2 >= s.length()) {
                    throw ConfigErrors.malformedUrl("a parameter has an incomplete %-escape");
                }
                int hi = Character.digit(s.charAt(i + 1), 16);
                int lo = Character.digit(s.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) {
                    throw ConfigErrors.malformedUrl("a parameter has an invalid %-escape");
                }
                bytes.write(hi * 16 + lo);
                i += 3;
            } else {
                byte[] b = new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8);
                bytes.write(b, 0, b.length);
                i += Character.charCount(cp);
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes.toByteArray()))
                    .toString();
        } catch (CharacterCodingException e) {
            throw ConfigErrors.malformedUrl("a parameter's %-escapes aren't valid UTF-8");
        }
    }
}
