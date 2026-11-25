package io.github.ifnesi.kafcacheql.testkit;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class LogCaptureTest {
    /** Captures record, and may nest; they never hide a warning from the console. */
    @Test void nestedCapturesRecordAndEveryWarningStillPrints() {
        var logger = LoggerFactory.getLogger(LogCaptureTest.class);
        var printed = new ByteArrayOutputStream();
        var stderr = System.err;
        try (var output = new PrintStream(printed, true, StandardCharsets.UTF_8);
             var outer = LogCapture.capture()) {
            System.setErr(output);
            logger.warn("outer one");
            LogCapture.start();
            List<String> inner;
            try {
                logger.warn("inner");
            } finally { inner = LogCapture.stop(); }
            logger.warn("outer two");
            assertEquals(1, inner.size());
            assertEquals(3, outer.lines().size());
            String visible = printed.toString(StandardCharsets.UTF_8);
            for (String text : List.of("outer one", "inner", "outer two")) assertTrue(visible.contains(text), text);
        } finally { System.setErr(stderr); }
    }
}
