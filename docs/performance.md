# Performance measurement

Run against the already running Docker stack (Kafka `localhost:9092`, Schema Registry
`localhost:8081`):

```sh
mvn -o -Dmaven.repo.local=$HOME/.cache/jdbc-kafka/tmp/m2 \
  test-compile failsafe:integration-test failsafe:verify \
  -Dit.test=PerformanceMeasurement -DargLine="-Xms1g -Xmx4g"
```

`PerformanceMeasurement` deliberately matches neither `*Test` nor `*IT`, so normal
`mvn verify` does not run it. It uses DriverManager and the ordinary driver settings;
the heap flag applies to the measurement JVM. It creates only two uniquely named
`perf-` topics, three partitions each, replication factor one, and deletes those exact
topics in `finally`. It never stops or reconfigures the stack. Schema Registry subjects
are left alone.

The compacted topic has 1,000,000 distinct string keys; the append topic has 3,000,000
records. Both have an Avro value containing a long ID, string status, double amount and
boolean. An asynchronous, acknowledged producer seeds the topics before timing queries.
Keys are assigned evenly to partitions. Since 0013c, record timestamps advance by 2 ms
per ID, ending about a minute before seeding starts: the append data spans 100 minutes.
The compacted delta updates 1,000 existing keys;
the append delta adds 1,000 keys. Counts, ID sums and updated-value counts are checked.

Each full query is `SELECT count(*),sum(id),sum(CASE WHEN status='updated' THEN 1 ELSE 0 END)
FROM "topic" WHERE _offset>=0`: it synchronizes the entire topic and scans its typed view.
The same connection executes the first, unchanged and delta queries. An additional cold
append `SELECT *` drains the default window (30,000 rows across three partitions), on
a separate cache, to measure the default-window criterion. That cache is reused for
an exact timestamp lower bound covering the newest 90,000 rows (60,000 not cached),
its repeat, and an uncached historical window: `_offset>=0 AND _offset<10000`
(30,000 rows across the partitions, older than the default window). Two connections
then start together on another uncached window, `_offset>=10000 AND _offset<20000`:
30,000 rows each, with one shared sync. Full caches are closed and reopened with
`offline=true`, without bootstrap servers, for the full aggregate again. No GUI is automated.

Artifacts stay under `target/performance-*/`: cache databases, `queries.csv`, and JDK
Flight Recorder profiles for each first sync and the cold default-window query. Caches
are on the filesystem containing `target`, not `/tmp` (which can be a RAM filesystem).
Seeding and connection-open times print separately. Query timers include execute, row
consumption and correctness checks, but exclude JFR startup/dumping. Profiling itself
adds overhead. There are no performance pass/fail thresholds or repeated-run averages.

CSV phases come from existing sync hooks: before the first fetch; fetch/decode/wait
between that point and the last commit, excluding commit time; total SQLite commit
time; and the remaining time after the last commit (checkpoint, query preparation and
SQL included). Checkpoint time is also shown separately and overlaps those intervals.
For a query with no fetch, all elapsed time is in `after_sync_s`. The Admin proxy counts
driver API calls, not physical network packets. File sizes include the live main DB
and WAL separately; closed-cache sizes also print after connection/context release.

Since 0013c, a test-scope `MetricsReporter` records before/after deltas of the consumer's
own `consumer-fetch-manager-metrics` counters `bytes-consumed-total` and
`records-consumed-total`. Only the aggregate metrics are counted, excluding their
per-topic copies; producer and Admin metrics are excluded. These are Kafka consumer
record-byte totals, not socket traffic or estimates from row sizes. Kafka 8.4.0-ccs
publishes these totals when all partitions of a fetch response are drained or discarded
(`CompletedFetch` / `FetchMetricsAggregator`), so a partial-window query can leave
metrics to be published by a later query. The CSV keeps the raw counter deltas and
`committed_records` separately: they need not match for partial windows. A cache-only
repeat must advance neither counter. Concurrent queries share one consumer, so their
fetch counters and CSV phase timings are reported once for the pair; each caller's
elapsed time also prints separately, with no arbitrary split of the shared fetch.

`heap_before_bytes` and `heap_after_bytes` come from `MemoryMXBean` immediately
around each query (or concurrent pair). They are used JVM heap, including the producer,
JDBC, profiler and harness, not process RSS or peak live driver memory. There is no
forced GC; a decrease can reflect a collection. Sampling and reporting are outside
the elapsed query timer.

Inspect profiles with JDK tools, for example:

```sh
jfr summary target/performance-*/perf-compact-*-first.jfr
jfr print --events jdk.ExecutionSample,jdk.NativeMethodSample \
  target/performance-*/perf-compact-*-first.jfr
```

## Recorded run

2026-11-25, one run (no tuning), Confluent Platform 8.4.0. AMD Ryzen 7 260,
8 physical cores / 16 logical CPUs, 53 GiB RAM, NVMe/ext4 cache filesystem.
OpenJDK 21.0.13.1; measurement JVM `-Xms1g -Xmx4g` (reported maximum
4,294,967,296 bytes); broker `-Xms1G -Xmx1G`. The Docker containers had no
CPU or memory limits. Kafka, Schema Registry and Control Center were already
running on the same machine. This is a warm OS/JIT environment, not a cold-machine
or concurrent-client benchmark.

| Topic | Initial records/keys | First full query | Unchanged | After 1,000-record delta | Closed cache after delta |
|---|---:|---:|---:|---:|---:|
| Compacted | 1,000,000 | 19.166 s | 0.413 s | 1.521 s | 164,569,088 B (156.95 MiB) |
| Append | 3,000,000 | 93.559 s | 1.270 s | 2.529 s | 439,775,232 B (419.40 MiB) |

Unchanged queries fetched **zero records**, committed zero batches, and each made
one `describeTopics`, one start `listOffsets`, and one end `listOffsets` call;
no `describeConfigs`. Delta queries fetched exactly 1,000 records in three batches.
First queries fetched exactly 1M / 3M records in 201 / 600 batches. Their initial
discovery also made one `listTopics`, two `describeTopics`, and one `describeConfigs`
call, plus the start/end lookups. Topic seeding took 1.571 s / 2.977 s; connection
opening took 0.168 s / 0.009 s, outside the query timers. Closed caches had no WAL;
live WAL sizes were 5,158,272 B / 5,079,992 B (not included in the closed sizes).

Comparison with §18's targets:

| Criterion | Measurement |
|---|---|
| Up-to-date cache adds at most one offsets round trip | The measured API calls are **two** offset lookups plus one topic description, following the current §9/§11 freshness contract; the literal single-round-trip target is not met. No fetch or config lookup. The 0.413 s / 1.270 s totals include a full SQL aggregate, not just Kafka overhead. |
| Compacted 1M keys usable after first sync | First sync and aggregate completed in 19.166 s; the same aggregate over the up-to-date cache took 0.413 s. JDBC was checked; DBeaver itself was not run. |
| Large append double-click returns within seconds via default window | A separate empty cache's ordinary `SELECT *` fetched and drained 30,000 rows in **0.349 s**, with the default-window warning. This checks the JDBC path, not GUI rendering. Closed default-window cache: 4,526,080 B. |

The much longer full append sync deliberately bypasses the default window. Phase
timings identify its cost; no driver changes were made:

| First full query | Before first fetch | Fetch/decode/wait | SQLite commits | After final commit |
|---|---:|---:|---:|---:|
| Compacted | 0.293 s | 3.430 s | 15.016 s | 0.427 s |
| Append | 0.032 s | 7.336 s | 84.914 s | 1.277 s |

JFR execution/native samples included `TypedViews.ids` in **126/190** compacted
sync-worker samples and **755/869** append sync-worker samples. That method runs
`SELECT DISTINCT _key_schema_id, _value_schema_id` over the cached table during
each batch's `TypedViews.refresh`; repeated scans of the growing table dominate
the observed sync-worker stacks. These are sample counts, not exact CPU-time
percentages. The append main-thread samples showed SQL aggregate execution (10)
and another `TypedViews.ids` scan during query preparation (3). Explicit worker
checkpoint hooks recorded zero time; SQLite's automatic checkpoints are included
in commit time. Delta commit times were 0.297 s / 0.775 s, with another
0.403 s / 1.262 s after the last commit for the aggregate/preparation.

Raw CSV, profiles and caches from this run:
`target/performance-1005341224842898274/`. Topics `perf-compact-e62fc200` and
`perf-append-ee2be923` were both deleted successfully. The measurement passed
(one test, no failures/errors/skips).

The subsequent normal command
`mvn -o -Dmaven.repo.local=$HOME/.cache/jdbc-kafka/tmp/m2 verify` passed:
214 unit tests and 102 integration tests, zero failures/errors/skips, 2 min 46 s.
It did not run `PerformanceMeasurement`.

## 0010b rerun: incremental schema-ID tracking

2026-11-25, same command, machine, heap, dataset and driver settings. The view's
persisted signature supplies the previous schema IDs, the batch contributes its
own IDs while records are written, and indexed existence checks remove IDs whose
last record was replaced/deleted/pruned. No process keeps a mutable ID cache.
Readers use the signature in their snapshot too. The two partial schema-ID indexes
raise the cache format from 5 to 6 and add storage overhead.

| Topic | First full query (before → after) | Unchanged | After 1,000-record delta | Closed cache after delta |
|---|---:|---:|---:|---:|
| Compacted, 1M keys | 19.166 → 11.179 s | 0.326 s | 1.346 s | 178,614,272 B (170.34 MiB) |
| Append, 3M records | 93.559 → 19.792 s | 1.028 s | 2.023 s | 482,230,272 B (459.89 MiB) |

First-sync commit time fell from 15.016 to **7.147 s** (compacted), and from
84.914 to **10.726 s** (append). New JFR profiles have no sync-worker samples in
schema-ID presence checks; the full-table `TypedViews.ids` scan no longer exists.
The cold default-window query fetched/drained 30,000 rows in **0.344 s**. Unchanged
queries still fetched zero records and made one topic description plus start/end
offset lookups; delta queries fetched exactly 1,000 records in three batches.

This rerun passed. CSV, profiles and caches are in
`target/performance-2108440504065318406/`. Both new topics,
`perf-compact-b7c0cd3a` and `perf-append-ccf736c4`, were deleted successfully.
The preceding format-5 cache artifacts are historical; the new driver refuses
them as it does any unsupported cache format.

The subsequent required offline `mvn verify` (including 0001c's native-library
filters) passed: 214 unit tests and 103 integration tests, no failures/errors/skips,
2 min 52 s. The opt-in measurement remained excluded from that run.


## 0013c run: fetch metrics, heap and more query shapes

2026-11-26, one run with the command above, Confluent Platform 8.4.0. AMD Ryzen 7
260, 8 physical cores / 16 logical CPUs, 53.7 GiB RAM, NVMe/ext4 cache filesystem;
OpenJDK 21.0.13.1, measurement JVM `-Xms1g -Xmx4g` (maximum 4,294,967,296 B).
The broker used `-Xms1G -Xmx1G` and had no container CPU or memory limit. The existing Docker stack stayed
running throughout. OS/JIT caches were warm. No tuning or product changes were made
for this item. Cache size and retention settings were unset.

| Topic / query | Seconds | Kafka consumer bytes | Kafka consumer records | Heap before → after (MiB) |
|---|---:|---:|---:|---:|
| Compacted 1M, first | 10.818 | 39,695,592 | 1,000,000 | 467.76 → 235.89 |
| Compacted, unchanged | 0.331 | 0 | 0 | 235.89 → 239.89 |
| Compacted, 1,000 updates | 1.348 | 39,173 | 1,000 | 241.89 → 253.89 |
| Compacted, offline full aggregate | 0.326 | 0 | 0 | 255.89 → 257.89 |
| Append 3M, first | 18.937 | 123,261,458 | 3,000,000 | 136.45 → 535.64 |
| Append, unchanged | 1.093 | 0 | 0 | 535.64 → 537.64 |
| Append, 1,000 additions | 2.087 | 45,775 | 1,000 | 541.64 → 553.64 |
| Append, offline full aggregate (3,001,000 rows) | 1.091 | 0 | 0 | 555.64 → 557.64 |

The unchanged full queries avoided the first queries' 39,695,592 B / 123,261,458 B
and 1M / 3M records. Each made one topic description and start/end offset lookups,
with no config call, meeting the current §18 up-to-date-cache criterion. Offline
queries opened no Kafka clients and used no Admin calls. Closed full caches after
the delta were 178,163,712 B (compacted) and 480,178,176 B (append), with no WAL.

The separate append window cache started empty. Its timestamp-bound query selected
the newest 90,000 IDs; the historical query selected the oldest 30,000. Both checked
counts and ID sums. The concurrent pair selected the next oldest 30,000 IDs each,
on two connections to this same cache.

| Append query | Seconds | Rows returned | Records committed | Kafka consumer bytes | Kafka consumer records | Heap before → after (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| Cold default window | 0.305 | 30,000 | 30,000 | 836,100 | 20,000 | 477.00 → 669.00 |
| Timestamp lower bound | 0.395 | 90,000 | 60,000 | 2,090,400 | 50,000 | 669.00 → 400.39 |
| Timestamp repeat | 0.042 | 90,000 | 0 | 0 | 0 | 400.39 → 402.39 |
| Uncached historical window | 0.205 | 30,000 | 30,000 | 1,599,662 | 40,000 | 402.39 → 574.39 |
| Concurrent pair, shared totals | 0.206 | 60,000 | 30,000 | 1,158,147 | 30,000 | 576.39 → 134.45 |

The concurrent callers took 0.205843 s and 0.205745 s individually; their common
window was committed once, in six batches. Pair totals include two topic descriptions
and two sets of start/end lookups; the new connection needed one config lookup.
Timestamp queries, including the cached repeat, each made one `offsetsForTimes`
lookup in addition to freshness checks (§8.3). Partial-window consumer counts differ
from commit counts because of Kafka's deferred response-level metrics described
above; they are reported as observed, without estimating missing bytes or records.

The default-window result remains within seconds (§18's JDBC-side target); GUI
rendering was not measured. First full-sync phase timings were:

| Topic | Before first fetch | Fetch/decode/wait | SQLite commits | After final commit |
|---|---:|---:|---:|---:|
| Compacted | 0.179 s | 3.405 s | 6.900 s | 0.333 s |
| Append | 0.010 s | 7.677 s | 10.152 s | 1.098 s |

Sampled used heap ranged from 134.45 to 669.00 MiB; this is not a peak-memory
measurement. No new driver defect was observed. Kafka's deferred metric publication
limits attribution of partial-window traffic to an individual query, and the shared
consumer's concurrent fetch is attributed to the pair.

CSV (including exact heap bytes), profiles and caches:
`target/performance-14131334537662465536/`. Console log:
`/home/inesi/.cache/jdbc-kafka/tmp/s6-fuller-performance.log`.
Topics `perf-compact-9ad5f76c` and `perf-append-604fb47e` were deleted successfully
in `finally`. The opt-in measurement passed (one test, no failures/errors/skips).

The subsequent normal command
`mvn -o -Dmaven.repo.local=$HOME/.cache/jdbc-kafka/tmp/m2 verify` passed:
223 unit tests and 106 integration tests, zero failures/errors/skips, 2 min 57 s.
It did not run `PerformanceMeasurement`. Log:
`/home/inesi/.cache/jdbc-kafka/tmp/s6-fuller-verify.log`.
