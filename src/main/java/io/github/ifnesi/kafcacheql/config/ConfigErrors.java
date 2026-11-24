package io.github.ifnesi.kafcacheql.config;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;

/**
 * Configuration errors fail {@code connect()}. Messages name the property, never a value read from the URL
 * or a client setting, since those can hold credentials.
 */
public final class ConfigErrors {

    /** SQLState for "SQL client unable to establish SQL connection". */
    public static final String SQLSTATE = "08001";

    private ConfigErrors() {}

    public static SQLException malformedUrl(String reason) {
        return new SQLNonTransientConnectionException(
                "Malformed KafCacheQL URL: " + reason + ". Expected " + JdbcUrl.FORM, SQLSTATE);
    }

    /** A driver property with a bad value; driver properties hold no credentials, so the value is shown. */
    public static SQLException invalid(String key, String value, String expected) {
        return new SQLNonTransientConnectionException(
                "Invalid value for " + key + ": '" + value + "' (expected " + expected + ")", SQLSTATE);
    }

    public static SQLException problem(String message) {
        return new SQLNonTransientConnectionException(message, SQLSTATE);
    }

    public static SQLException problem(String message, Throwable cause) {
        return new SQLNonTransientConnectionException(message, SQLSTATE, cause);
    }
}
