package io.github.ifnesi.kafcacheql.util;

import java.util.HexFormat;

/** Visible control characters for single-line diagnostics and terminal cells. */
public final class ControlCharacters {
    private static final HexFormat HEX = HexFormat.of();
    private ControlCharacters() {}

    public static String escape(String value) {
        if (value == null) return null;
        var escaped = new StringBuilder(value.length());
        value.codePoints().forEach(c -> {
            switch (c) {
                case '\r' -> escaped.append("\\r");
                case '\n' -> escaped.append("\\n");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (Character.isISOControl(c)) escaped.append("\\x").append(HEX.toHexDigits((byte) c));
                    else escaped.appendCodePoint(c);
                }
            }
        });
        return escaped.toString();
    }
}
