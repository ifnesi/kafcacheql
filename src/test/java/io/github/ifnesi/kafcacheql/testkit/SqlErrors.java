package io.github.ifnesi.kafcacheql.testkit;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

/** Shared JDBC/CLI oracles for the supported-statement grammar and readable SQL errors. */
public final class SqlErrors {
    private SqlErrors() {}
    public record BadInput(String sql, String message, String state) {
        public BadInput(String sql, String message) { this(sql, message, "42000"); }
    }
    private static final String HINT = " Quote names that contain '-' or '.', such as \"my-topic\".";

    public static List<BadInput> badInputs() {
        return List.of(
                new BadInput("ssss;", unknown("ssss")),
                new BadInput("SELEC * FROM x", unknown("SELEC")),
                new BadInput("INSERT INTO x VALUES (1)", unknown("INSERT")),
                new BadInput("UPDATE x SET n=1", unknown("UPDATE")),
                new BadInput("CREATE TABLE x (n INT)", unknown("CREATE")),
                new BadInput("describe shop-x", "SQL syntax error at line 1, column 14: unexpected token '-'." + HINT),
                new BadInput("SELECT * FROM shop-x", "SQL syntax error at line 1, column 19: unexpected token '-'." + HINT),
                new BadInput("SELECT FROM", "SQL syntax error at line 1, column 1: unexpected token 'SELECT'."),
                new BadInput("SELECT * FROM \"orders\" WHERE", "SQL syntax error at line 1, column 24: unexpected token 'WHERE'."),
                new BadInput("SELECT * FROM \"orders\"\nWHERE  ;  ", "SQL syntax error at line 2, column 1: unexpected token 'WHERE'."),
                new BadInput("DROP VIEW x", "Only DROP TABLE is supported."),
                new BadInput("SHOW TABLESS", "SQL syntax error at line 1, column 6: unexpected token 'TABLESS'."),
                new BadInput("SELECT 1 )", "SQL syntax error at line 1, column 10: unexpected token ')'."),
                new BadInput("SELECT 1 2", "SQL syntax error at line 1, column 10: unexpected token '2'."),
                new BadInput("SELECT * FROM \"orders", unclosed()),
                new BadInput("SELECT 'PAID", unclosed()),
                new BadInput("SELECT /* x", unclosed()),
                new BadInput("SELECT § 1", "Invalid SQL syntax."),
                new BadInput(";", "Empty statement."),
                new BadInput("", "Empty statement."),
                new BadInput("SELECT * FROM shop-orders", "SQL syntax error at line 1, column 19: unexpected token '-'." + HINT),
                new BadInput("SELECT count(* FROM \"orders\"", "SQL syntax error at line 1, column 16: unexpected token 'FROM'."),
                new BadInput("SYNC \"orders\" \"customers\"", sync()),
                new BadInput("SYNC ,", sync()),
                new BadInput("SHOW TABLES orders,", "SHOW TABLES expects topic names or patterns separated by commas, such as SHOW TABLES orders, shop-*."),
                new BadInput("SHOW TABLES \"orders\" \"customers\"", "SHOW TABLES expects topic names or patterns separated by commas, such as SHOW TABLES orders, shop-*."),
                new BadInput("SHOW TABLES ,", "SHOW TABLES expects topic names or patterns separated by commas, such as SHOW TABLES orders, shop-*."),
                new BadInput("SELECT nosuchcol", "no such column: nosuchcol.", null),
                new BadInput("SELECT 1; SELECT 2", "Only one statement at a time is supported."));
    }

    private static String unclosed() {
        return "SQL syntax error: the statement ends inside a quoted name, string or comment that isn't closed.";
    }

    private static String sync() { return "SYNC expects topic names or patterns separated by commas, such as SYNC orders, shop-*."; }

    /** Use for calls through the driver's public JDBC objects (including worker futures from such calls).
     * Raw SQLite and internal cache/worker contracts use JUnit's ordinary assertions. Original causes
     * remain diagnostics for logs; only the SQLException returned to the JDBC caller is a user message. */
    public static <T extends Throwable> T assertJdbcThrows(Class<T> type, org.junit.jupiter.api.function.Executable work) {
        return assertJdbcThrows(type, work, "");
    }
    public static <T extends Throwable> T assertJdbcThrows(Class<T> type, org.junit.jupiter.api.function.Executable work, String message) {
        T error = org.junit.jupiter.api.Assertions.assertThrows(type, work, message);
        for (Throwable observed = error; observed != null; observed = observed.getCause()) {
            if (observed instanceof SQLException sql) { assertReadable(sql.getMessage()); break; }
        }
        return error;
    }

    private static String unknown(String word) {
        return "Unknown statement \"" + word + "\": only SELECT, WITH ... SELECT, SHOW TABLES, DESCRIBE, DROP TABLE and SYNC are supported.";
    }

    public static void assertMessage(BadInput input, SQLException error) {
        assertReadable(error.getMessage());
        assertEquals(input.state(), error.getSQLState(), input.sql());
        assertEquals(input.message(), error.getMessage(), input.sql());
    }

    public static void assertReadable(String message) {
        assertNotNull(message);
        assertFalse(java.util.regex.Pattern.compile("(?:[\\w$]+\\.)*[\\w$]+(?:Exception|Error):").matcher(message).find(), message);
        String lower = message.toLowerCase(Locale.ROOT);
        for (String internal : List.of("shaded", "jsqlparser", "exception:", "expansion", "[sqlite_")) {
            assertFalse(lower.contains(internal), message);
        }
    }
}
