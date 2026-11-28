package io.github.ifnesi.kafcacheql.cli;

import com.google.common.base.Ascii;
import io.github.ifnesi.kafcacheql.cli.Results.Mode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.CompletingParsedLine;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;

/** Supplies candidates and JDBC identifier escaping; JLine does all matching and insertion. */
final class Completions extends DefaultParser implements Completer, AutoCloseable {
    private static final List<Candidate> STARTERS = List.of("SELECT", "WITH", "SHOW TABLES", "DESCRIBE",
            "SYNC", "DROP TABLE", "help", "quit", "exit", "mode").stream().map(Candidate::new).toList();
    private static final List<Candidate> WORDS = List.of("SELECT", "FROM", "WHERE", "ORDER BY", "GROUP BY",
            "LIMIT", "HAVING", "JOIN", "LEFT JOIN", "ON", "AS", "AND", "OR", "WITH", "UNION ALL",
            "SHOW TABLES", "DESCRIBE", "SYNC", "DROP TABLE", "help", "quit", "exit", "mode")
            .stream().map(Candidate::new).toList();
    private static final List<Candidate> MODES = Arrays.stream(Mode.values()).map(Mode::toString).map(Candidate::new).toList();
    private final Connection connection;
    private final BooleanSupplier newStatement;
    private final ScheduledExecutorService refresh = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon(true).name("kafcacheql-completions").factory());
    private record Topics(Map<String, String> quoted, List<Candidate> candidates) {}
    private volatile Topics topics = new Topics(Map.of(), List.of());

    Completions(Connection connection, BooleanSupplier newStatement) {
        this.connection = connection;
        this.newStatement = newStatement;
        escapeChars(new char[0]);
        refresh();
        // Test-only clock interval, not a connection setting. Normal sessions refresh at most once a minute.
        long interval = Long.getLong("kafcacheql.test.completion.refresh.ms", 60_000L);
        refresh.scheduleWithFixedDelay(this::refresh, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void refresh() {
        try {
            var metadata = connection.getMetaData();
            String quote = metadata.getIdentifierQuoteString();
            var names = new HashMap<String, String>();
            try (var rows = metadata.getTables(null, null, null, new String[]{"TABLE"})) {
                while (rows.next()) {
                    String name = rows.getString("TABLE_NAME");
                    if (!name.startsWith("$")) names.put(name, quote + name.replace(quote, quote + quote) + quote);
                }
            }
            topics = new Topics(Map.copyOf(names), names.keySet().stream().map(Candidate::new).toList());
        } catch (SQLException unavailable) {
            // Completion is disposable: retain the last successful list, without terminal output.
        }
    }

    @Override public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        var parsed = (CompletionLine) line;
        if (parsed.firstWord) {
            candidates.addAll(STARTERS);
        } else if (parsed.modeNames) {
            candidates.addAll(MODES);
        } else {
            candidates.addAll(WORDS);
            candidates.addAll(parsed.names.candidates());
        }
    }

    @Override public ParsedLine parse(String line, int cursor, ParseContext context) {
        var parsed = (CompletingParsedLine) super.parse(line, cursor, context);
        return new CompletionLine(parsed, topics);
    }

    private final class CompletionLine extends ArgumentList {
        private final CompletingParsedLine parsed;
        private final Topics names;
        private final boolean firstWord;
        private final boolean modeNames;

        CompletionLine(CompletingParsedLine parsed, Topics names) {
            super(parsed.line(), parsed.words(), parsed.wordIndex(), parsed.wordCursor(), parsed.cursor(),
                    null, parsed.rawWordCursor(), parsed.rawWordLength());
            this.parsed = parsed;
            this.names = names;
            firstWord = newStatement.getAsBoolean() && parsed.wordIndex() == 0;
            modeNames = newStatement.getAsBoolean() && parsed.wordIndex() == 1
                    && Ascii.equalsIgnoreCase(parsed.words().getFirst(), "mode");
        }

        @Override public CharSequence escape(CharSequence candidate, boolean complete) {
            String quoted = firstWord || modeNames ? null : names.quoted().get(candidate.toString());
            return complete && quoted != null ? quoted : parsed.escape(candidate, complete);
        }
    }

    @Override public void close() {
        refresh.shutdownNow();
        refresh.close();
    }
}
