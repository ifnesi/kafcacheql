# Performance

The first query fills a local SQLite cache; later queries reuse those records. These measurements show that
difference, then compare smaller windows (selected record ranges) and parallel fetching (reading several
partitions at once).

All results are single runs on one machine, not averages or benchmarks. Use them for orders of magnitude.

## What was measured

**The data:** a compacted topic with the current state of 1,000,000 keys and an append topic with a history of
3,000,000 records, three partitions each, with an Avro value (a long ID, a string status, a double amount and
a boolean). The append records span 100 minutes. Each query's counts and ID sums are checked.

**The queries:** a full aggregate (`SELECT count(*), sum(id), … FROM "topic" WHERE _offset>=0`), which syncs
the whole topic, then the same query unchanged, after 1,000 new records, and offline (from the cache without
Kafka). On the append topic also: a `SELECT *` with no bounds and an explicit `append.default.window=1h`, a
timestamp bound, a historical offset range, and two connections asking for the same range at once.

**The machine:** AMD Ryzen 7 260 (8 cores, 16 threads), 53.7 GiB RAM, NVMe disk; OpenJDK 21; Confluent
Platform 8.4.0 in Docker on the same machine, broker heap 1 GiB. Measured 2026-11-26.

## Full topics

Measured with one consumer, before parallel fetching (below):

| Topic / query | Seconds | Bytes fetched from Kafka | Records fetched |
|---|---:|---:|---:|
| Compacted 1M keys, first query | 10.8 | 39,695,592 | 1,000,000 |
| Compacted, unchanged | 0.33 | 0 | 0 |
| Compacted, after 1,000 updates | 1.35 | 39,173 | 1,000 |
| Compacted, offline | 0.33 | 0 | 0 |
| Append 3M records, first query | 18.9 | 123,261,458 | 3,000,000 |
| Append, unchanged | 1.09 | 0 | 0 |
| Append, after 1,000 new records | 2.09 | 45,775 | 1,000 |
| Append, offline | 1.09 | 0 | 0 |

An unchanged query fetches nothing: it asks Kafka for the topic's description and its start and end offsets,
then runs the SQL. Most of a first query's time is SQLite writing the records (6.9 s of the compacted topic's
10.8 s, 10.2 s of the append topic's 18.9 s). The closed caches were 178 MB (compacted) and 480 MB (append).

## Append-topic windows

A window limits how much of an append topic a query reads. This run shows the cost of reading a small window
first, then fetching other ranges and reusing records already cached.

On a separate, empty cache with `append.default.window=1h` and `append.default.window.max.records=10000`.
These are measurements of an opt-in window; the [default is `all`](configuration.md#the-drivers-own-settings).

| Query | Seconds | Rows returned | Records fetched |
|---|---:|---:|---:|
| `SELECT *`, no bounds (explicit 1h window) | 0.31 | 30,000 | 30,000 |
| Timestamp lower bound, newest 90,000 records | 0.40 | 90,000 | 60,000 (the rest were cached) |
| The same, repeated | 0.04 | 90,000 | 0 |
| Historical offset range | 0.21 | 30,000 | 30,000 |
| Two connections, same new range at once | 0.21 | 30,000 each | 30,000 (fetched once) |

## Parallel fetch

A first full query of a six-partition append topic of 3,000,000 records, with `sync.fetch.parallelism` 1 and 4
(the default). Each fetching thread also decodes its own records; Schema Registry is still asked once per
schema ID. SQLite has one writer, so writing stays sequential:

| Consumers | First full query | Fetching and decoding | SQLite writing |
|---|---:|---:|---:|
| 1 | 23.9 s | 9.8 s | 12.9 s |
| 4 | 15.9 s | 2.7 s | 12.1 s |

Four consumers were 1.5 times faster here. SQLite writing accounted for most of the remaining time. Results on
other machines or remote clusters will differ.

## Running the measurement

To repeat these scenarios on your machine, run the two measurements below. The first compares full topics and
query windows; the second compares one and four consumers.

With the [quick start](../README.md#quick-start)'s stack running, from the repository's top folder, `kafcacheql`:

```sh
mvn test-compile failsafe:integration-test failsafe:verify \
  '-Dit.test=PerformanceMeasurement#measure' -Dtest.heap=4g
mvn test-compile failsafe:integration-test failsafe:verify \
  '-Dit.test=PerformanceMeasurement#parallelFetch' -Dtest.heap=4g
```

`PerformanceMeasurement` isn't part of `mvn verify`. It creates its own uniquely named `perf-` topics, deletes
them when it ends, and leaves the stack running. Results, cache files and JDK Flight Recorder execution
profiles go under `target/performance-*/`.
