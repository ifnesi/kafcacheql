package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.schema.RecordDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/** One decoder owner per cache. Only the sync worker submits batches, with no poll pipelining. */
final class BatchDecoder implements AutoCloseable {
    private final ExecutorService executor;
    private final RecordDecoder decoder;
    private CompletableFuture<Void> idle = CompletableFuture.completedFuture(null);

    /** The reader is borrowed exclusively until close has drained and closed the decoder. */
    BatchDecoder(ClientSettings settings, Connection reader, String name) {
        decoder = new RecordDecoder(settings, reader);
        executor = Executors.newSingleThreadExecutor(work -> {
            Thread thread = Thread.ofPlatform().daemon().name(name).unstarted(work);
            thread.setContextClassLoader(DriverLoader.LOADER);
            return thread;
        });
    }

    /** Called before polling: a cancelled batch finishes before any subsequent poll. */
    void awaitIdle(Execution execution) throws SQLException {
        execution.await(idle);
    }

    List<SyncBatch.Record> decode(String topic, ConsumerRecords<byte[], byte[]> records, long end,
                                  Format key, Format value, Execution execution) throws SQLException {
        var result = new CompletableFuture<List<SyncBatch.Record>>();
        var finished = new CompletableFuture<Void>();
        idle = finished;
        executor.execute(() -> {
            try {
                List<SyncBatch.Record> decoded = new ArrayList<>(records.count());
                for (var record : records) {
                    if (record.offset() >= end) break;
                    decoded.add(new SyncBatch.Record(record.offset(), record.timestamp(), record.key(), record.headers(),
                            decoder.decode(topic, record.headers(), record.key(), record.value(), key, value)));
                }
                result.complete(decoded);
            } catch (SQLException | RuntimeException e) {
                result.completeExceptionally(e);
            } catch (Error e) {
                result.completeExceptionally(e);
                throw e;
            } finally {
                finished.complete(null);
            }
        });
        try {
            return execution.await(result);
        } finally {
            // Cancels only delivery. The runnable always finishes, retaining its shared schema cache.
            // A late completion cannot retain the abandoned decoded batch in this future.
            result.cancel(false);
        }
    }

    @Override public void close() throws SQLException {
        var closed = executor.submit(() -> { decoder.close(); return null; });
        executor.shutdown();
        boolean interrupted = false;
        try {
            while (true) {
                try { closed.get(); break; }
                catch (InterruptedException e) { interrupted = true; }
                catch (java.util.concurrent.ExecutionException e) {
                    if (e.getCause() instanceof SQLException sql) throw sql;
                    if (e.getCause() instanceof Error error) throw error;
                    throw new SQLException("Closing decoder", e.getCause());
                }
            }
        } finally {
            executor.close();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
