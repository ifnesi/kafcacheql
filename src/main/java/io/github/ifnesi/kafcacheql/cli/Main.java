package io.github.ifnesi.kafcacheql.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.SQLWarning;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.spi.SystemStream;
import org.jline.terminal.spi.TerminalProvider;
import org.slf4j.helpers.NOP_FallbackServiceProvider;

/** A plain JDBC client. Loading the driver as a library never loads this entry point. */
public final class Main {
    private static final String HELP = """
            Statements end with ';' at the end of a line. Quote names that contain '-' or '.'.

              SELECT ... ;               Query topics as tables; fetches what the query needs first
              SHOW TABLES;               Topics, with cached rows and bytes
              DESCRIBE "<table>";        A table's columns and types
              SYNC [<pattern>, ...];     Fetch everything new: cached topics, or names with * and ?
              DROP TABLE "<table>";      Remove a topic from the local cache; Kafka is unchanged

              help                       This list (no ';' needed)
              quit, exit                 Leave (or Ctrl-D)
              Ctrl-C                     Cancel the running statement
              Up and down arrows         Earlier statements
              Ctrl-R                     Search earlier statements
              Ctrl-A, Ctrl-E             Start, end of the line

            Docs: https://github.com/ifnesi/kafcacheql/blob/main/docs/cli.md
            """;
    private volatile Statement running;
    private boolean failed;
    private boolean session;
    private final boolean json;

    private Main(boolean json) { this.json = json; }

    public static void main(String[] args) {
        // Explicit SPI selection, only in this executable: library users keep their host's provider.
        System.setProperty("slf4j.provider", NOP_FallbackServiceProvider.class.getName());
        System.setProperty("slf4j.internal.verbosity", "ERROR");
        System.exit(run(args));
    }

    private static int run(String[] args) {
        int url = args.length > 0 && args[0].equals("--json") ? 1 : 0;
        if (args.length <= url) {
            System.err.println("Usage: java -jar kafcacheql-jdbc.jar [--json] <jdbc-url> [statement ...]");
            return 1;
        }
        if (!args[url].startsWith("jdbc:kafcacheql:")) {
            System.err.println("ERROR: Not a KafCacheQL URL: it must start with jdbc:kafcacheql:");
            return 1;
        }
        var cli = new Main(url == 1);
        try (var connection = DriverManager.getConnection(args[url])) {
            warnings(connection.getWarnings()); connection.clearWarnings();
            if (args.length > url + 1) {
                for (int i = url + 1; i < args.length; i++) {
                    if (!cli.execute(connection, args[i])) break;
                }
            } else if (interactive()) {
                cli.interactive(connection);
            } else {
                cli.script(connection);
            }
            return cli.failed ? 1 : 0;
        } catch (SQLException | IOException error) {
            cli.error(error);
            return 1;
        }
    }

    private boolean execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            running = statement;
            try {
                if (statement.execute(sql)) {
                    try (var rows = statement.getResultSet()) { Results.print(rows, json); }
                }
            } finally {
                // A late terminal signal must never cancel the next statement.
                running = null;
                warnings(statement.getWarnings()); statement.clearWarnings();
                warnings(connection.getWarnings()); connection.clearWarnings();
            }
            return true;
        } catch (SQLException | IOException error) {
            failed = true;
            error(error);
            return false;
        }
    }

    private void script(Connection connection) throws IOException {
        var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        var sql = new StringBuilder();
        for (String line; (line = input.readLine()) != null;) {
            sql.append(line).append('\n');
            if (line.stripTrailing().endsWith(";")) {
                if (!execute(connection, sql.toString())) return;
                sql.setLength(0);
            }
        }
        if (!sql.toString().isBlank()) throw new IOException("Unterminated statement: expected ';' at the end of a line.");
    }

    private static boolean interactive() throws IOException {
        TerminalProvider provider = TerminalProvider.load("jni");
        return provider.isSystemStream(SystemStream.Input) && provider.isSystemStream(SystemStream.Output);
    }

    private void interactive(Connection connection) throws IOException, SQLException {
        session = true;
        var info = connection.getClientInfo();
        String connected = Boolean.parseBoolean(info.getProperty("offline")) ? "offline (cache only)"
                : "connected to " + info.getProperty("bootstrap.servers");
        Path cache = Path.of(info.getProperty("cache.path")), home = Path.of(System.getProperty("user.home"));
        String shown = cache.startsWith(home) ? "~" + cache.toString().substring(home.toString().length()) : cache.toString();
        System.out.println("KafCacheQL " + connection.getMetaData().getDriverVersion() + ", " + connected);
        System.out.println("Cache: " + shown);
        System.out.println("Type help for commands, quit to leave.");
        try (Terminal terminal = TerminalBuilder.builder().system(true).provider("jni").dumb(false).build()) {
            terminal.handle(Terminal.Signal.INT, signal -> {
                Statement active = running;
                if (active != null) {
                    try { active.cancel(); }
                    catch (SQLException error) { error(error); }
                }
            });
            LineReader reader = LineReaderBuilder.builder().terminal(terminal)
                    .parser(new DefaultParser().quoteChars(new char[0]).escapeChars(new char[0]))
                    .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                    .build(); // No HISTORY_FILE: DefaultHistory stays in memory.
            var sql = new StringBuilder();
            while (true) {
                try {
                    String line = reader.readLine(sql.isEmpty() ? "kafcacheql> " : "       ...> ");
                    if (sql.isEmpty() && line.strip().matches("(?i)(quit|exit);?")) return;
                    if (sql.isEmpty() && line.strip().matches("(?i)help;?")) {
                        System.out.print(HELP);
                        continue;
                    }
                    if (sql.isEmpty() && line.isBlank()) continue;
                    sql.append(line).append('\n');
                    if (line.stripTrailing().endsWith(";")) {
                        execute(connection, sql.toString());
                        sql.setLength(0);
                    }
                } catch (UserInterruptException interrupt) {
                    sql.setLength(0);
                } catch (EndOfFileException end) {
                    return;
                }
            }
        }
    }

    private static void warnings(SQLWarning warning) {
        for (; warning != null; warning = warning.getNextWarning()) {
            System.err.println("WARN: " + warning.getMessage());
        }
    }

    private void error(Exception error) {
        String message = error.getMessage();
        if (session && error instanceof SQLException sql && "42000".equals(sql.getSQLState())
                && message != null && message.startsWith("Unknown statement \"")) {
            message += " Type help for the list.";
        }
        System.err.println("ERROR: " + message);
    }
}
