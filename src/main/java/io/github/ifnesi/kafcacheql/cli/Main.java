package io.github.ifnesi.kafcacheql.cli;

import io.github.ifnesi.kafcacheql.cli.Results.Mode;
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
import java.util.regex.Pattern;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.spi.SystemStream;
import org.jline.terminal.spi.TerminalProvider;
import org.slf4j.helpers.NOP_FallbackServiceProvider;

/** A plain JDBC client. Loading the driver as a library never loads this entry point. */
public final class Main {
    private static final String USAGE = "Usage: java -jar kafcacheql-jdbc.jar [--mode table|line|json] <jdbc-url> [statement ...]";
    private static final Pattern MODE_COMMAND = Pattern.compile("(?i)mode(?:\\s+([^;]*?))?;?");
    private static final String HELP = """
            Statements end with ';' at the end of a line. Quote names that contain '-' or '.'.

              SELECT ... ;                  Query topics as tables; fetches what the query needs first
              SHOW TABLES [<pattern>, ...]; Topics, with cached rows and bytes
              DESCRIBE "<table>";           A table's columns and types
              SYNC [<pattern>, ...];        Fetch everything new: cached topics, or names with * and ?
              DROP TABLE "<table>";         Remove a topic from the local cache; Kafka is unchanged

              help                          This list (no ';' needed)
              mode [table|line|json]        Show or change how results print
              quit, exit                    Leave (or Ctrl-D)
              Ctrl-C                        Cancel the running statement
              TAB                           Complete keywords, topics and modes
              Up and down arrows            Earlier statements
              Ctrl-R                        Search earlier statements
              Ctrl-A, Ctrl-E                Start, end of the line

            Docs: https://github.com/ifnesi/kafcacheql/blob/main/docs/cli.md
            """;
    private volatile Statement running;
    private boolean failed;
    private boolean session;
    private Mode mode;
    private final Results results = new Results();

    private Main(Mode mode) { this.mode = mode; }

    public static void main(String[] args) {
        // Explicit SPI selection, only in this executable: library users keep their host's provider.
        System.setProperty("slf4j.provider", NOP_FallbackServiceProvider.class.getName());
        System.setProperty("slf4j.internal.verbosity", "ERROR");
        System.exit(run(args));
    }

    private static int run(String[] args) {
        int url = 0;
        Mode mode = Mode.TABLE;
        if (args.length > 0 && args[0].equals("--mode")) {
            try { mode = Mode.parse(args.length > 1 ? args[1] : ""); }
            catch (IllegalArgumentException invalid) { return usageError(invalid.getMessage()); }
            url = 2;
        }
        if (args.length > url && args[url].startsWith("--")) {
            return usageError("Unknown option: " + args[url] + ".");
        }
        if (args.length <= url) {
            System.err.println(USAGE);
            return 1;
        }
        if (!args[url].startsWith("jdbc:kafcacheql:")) {
            System.err.println("ERROR: Not a KafCacheQL URL: it must start with jdbc:kafcacheql:");
            return 1;
        }
        for (int i = url + 1; i < args.length; i++) {
            if (args[i].equals("--mode")) return usageError("Put --mode before the URL.");
        }
        var cli = new Main(mode);
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

    private static int usageError(String message) {
        System.err.println("ERROR: " + message);
        System.err.println(USAGE);
        return 1;
    }

    private boolean execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            running = statement;
            try {
                if (statement.execute(sql)) {
                    try (var rows = statement.getResultSet()) { results.print(rows, mode, session); }
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
        var sql = new StringBuilder();
        try (var completions = new Completions(connection, sql::isEmpty);
             Terminal terminal = TerminalBuilder.builder().system(true).provider("jni").dumb(false).build()) {
            terminal.handle(Terminal.Signal.INT, signal -> {
                Statement active = running;
                if (active != null) {
                    try { active.cancel(); }
                    catch (SQLException error) { error(error); }
                }
            });
            LineReader reader = LineReaderBuilder.builder().terminal(terminal)
                    .parser(completions).completer(completions)
                    .option(LineReader.Option.CASE_INSENSITIVE, true)
                    .option(LineReader.Option.AUTO_LIST, true)
                    .option(LineReader.Option.LIST_AMBIGUOUS, true)
                    .option(LineReader.Option.AUTO_MENU, false)
                    .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                    .build(); // No HISTORY_FILE: DefaultHistory stays in memory.
            while (true) {
                try {
                    String line = reader.readLine(sql.isEmpty() ? "kafcacheql> " : "       ...> ");
                    if (sql.isEmpty() && line.strip().matches("(?i)(quit|exit);?")) return;
                    if (sql.isEmpty() && line.strip().matches("(?i)help;?")) {
                        System.out.print(HELP);
                        continue;
                    }
                    var command = MODE_COMMAND.matcher(line.strip());
                    if (sql.isEmpty() && command.matches()) {
                        try {
                            if (command.group(1) != null) mode = Mode.parse(command.group(1).strip());
                            System.out.println("Mode: " + mode);
                        } catch (IllegalArgumentException invalid) {
                            failed = true;
                            error(invalid);
                        }
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
