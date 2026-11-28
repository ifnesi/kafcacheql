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

    /** Library causes stay attached for diagnostics; user messages use their plain detail only. */
    public static String detail(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        if (root instanceof java.nio.file.NoSuchFileException file) return file.getFile() + " doesn't exist";
        if (root instanceof java.nio.file.AccessDeniedException file) return "Permission denied for " + file.getFile();
        if (root instanceof java.nio.file.FileAlreadyExistsException file) return file.getFile() + " already exists";
        if (root instanceof java.nio.file.FileSystemException file && file.getReason() != null) return file.getReason();
        if (root instanceof InterruptedException) return "operation interrupted";
        String message = root.getMessage();
        if (message == null || message.isBlank()) {
            if (root instanceof java.util.concurrent.TimeoutException || root instanceof org.apache.kafka.common.errors.TimeoutException) return "timed out";
            return "operation failed";
        }
        if (root instanceof org.sqlite.SQLiteException) {
            // sqlite-jdbc exposes the native detail only inside this result-code wrapper.
            int start = message.indexOf(" (");
            if (message.startsWith("[SQLITE_") && start >= 0 && message.endsWith(")"))
                return message.substring(start + 2, message.length() - 1);
        }
        return message;
    }

    /** End user-facing sentences consistently, including messages assembled from a library's detail. */
    public static String sentence(String message) {
        if (message == null || message.isBlank()) return "Operation failed.";
        return message.endsWith(".") ? message : message + ".";
    }

    /** Keep SQLite's SQLState and vendor code, with its original exception available to logs. */
    public static SQLException plain(SQLException failure) {
        if (!(failure instanceof org.sqlite.SQLiteException)) return failure;
        return new SQLException(sentence(detail(failure)), failure.getSQLState(), failure.getErrorCode(), failure);
    }

    public static SQLException malformedUrl(String reason) {
        return new SQLNonTransientConnectionException(sentence(
                "Malformed KafCacheQL URL: " + reason + ". Expected " + JdbcUrl.FORM), SQLSTATE);
    }

    /** A driver property with a bad value; driver properties hold no credentials, so the value is shown. */
    public static SQLException invalid(String key, String value, String expected) {
        return new SQLNonTransientConnectionException(sentence(
                "Invalid value for " + key + ": '" + value + "' (expected " + expected + ")"), SQLSTATE);
    }

    public static SQLException problem(String message) {
        return new SQLNonTransientConnectionException(sentence(message), SQLSTATE);
    }

    public static SQLException problem(String message, Throwable cause) {
        return new SQLNonTransientConnectionException(sentence(message), SQLSTATE, cause);
    }
}
