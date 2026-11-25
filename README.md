# KafCacheQL

*Kafka + Cache + Query Language*

> **Work in progress.** This driver is in early development and **can't run queries yet**. Today it
> loads in JDBC tools and checks your configuration; reading topics comes in the next stages (see
> [Status](#status)). Nothing here is stable: names, settings and behaviour may change. Not for production use.

**What if you could query Kafka topics from your database tool of choice? No Kafka client, just a JDBC
driver. And best of all, whatever you query is cached locally, automatically.**

KafCacheQL is a read-only JDBC driver for Apache Kafka. Point DBeaver, DataGrip or any JVM application at a
Kafka cluster and query topics with plain SQL:

```sql
-- Planned: not working yet
SELECT status, count(*)
FROM "orders"
WHERE _timestamp >= unixepoch('now', '-15 minutes') * 1000
GROUP BY status;
```

The idea: each topic is a table. Records are fetched from Kafka once and kept in a local SQLite file, so the
next query only fetches what's new. Joins, aggregations and JSON functions are SQLite's.

## What it's for

- Looking at what's in a topic from the SQL tool you already use, without a Kafka UI or a consumer script.
- Ad hoc questions and joins across topics: reference data, configuration, the latest state of compacted
  topics.
- Checking topic contents in tests and while developing.
- Working with a topic's data offline, from the cache.

## What it isn't

- **Not real-time.** There are no continuous queries and no streaming results. A query answers from the cache
  as it stands once the query's data has been fetched; to see newer records, run the query again.
- **Not for production systems.** It's a tool for people exploring data, not a component to build services on.
- **Not a Kafka consumer replacement.** It has no consumer group and no processing guarantees. Applications
  that process events should use the Kafka clients or Kafka Streams.
- **Read-only.** SELECT only: no INSERT, UPDATE or DELETE, and it never produces to Kafka.
- **Not a database server.** Nothing runs besides your tool and Kafka (plus Schema Registry, when topics use it).

## How it works (planned)

1. A query names topics as tables: `"orders"`, `"payments.v1"`.
2. For each topic, the driver works out which offsets the query needs. Compacted topics are read up to their
   end; append topics are read for the window the query's `WHERE` clause asks for on `_partition`, `_offset`
   or `_timestamp`, or a default window (the last hour, at most 10,000 records per partition) when it doesn't
   say.
3. Only the offsets not yet cached are fetched, then decoded (Avro with Schema Registry, plain JSON, strings
   and bytes first; JSON Schema and Protobuf later) and stored as JSON in SQLite, with each record's
   `_partition`, `_offset`, `_timestamp`, `_key` and `_headers`.
4. SQLite runs the query. Each topic's table is a view with a column per field of its schema.

Compacted topics hold the latest value per key in each partition, as Kafka does, and tombstones delete keys. When the cache could have missed
deletions, or a topic was recreated, the topic is rebuilt.

## Status

| Stage | State |
|---|---|
| Build, shaded JAR, test environment | Done |
| Driver, URL and configuration | Done |
| Client settings, credentials kept out of logs and errors | Done |
| Kafka clients: cluster ID, topic metadata, offsets | Done |
| SQLite cache file and its internal tables | Done |
| Topic tables, decoding, syncing records into the cache | Done |
| Cache validity and rebuilds, SQL analysis, windows, query rewrite, typed views | Next |
| Connections, statements, result sets, database metadata in tools | Later |
| JSON Schema and Protobuf, offline mode | Later |

Until connections are built, `connect()` checks the URL and every setting, then stops with *"KafCacheQL
connections are not built yet; the configuration is valid"*.

## Trying it today

Requirements: Java 21 or newer, Maven 3.9 or newer.

```sh
mvn package -DskipTests
```

This builds `target/kafcacheql-jdbc-0.1.0-SNAPSHOT.jar`, one self-contained JAR (about 50 MB) with Kafka's and
Confluent's libraries relocated inside it, so it doesn't clash with other versions in the same application.

### In DBeaver

1. Copy the JAR somewhere stable; `mvn clean` deletes `target/`.
2. **Database → Driver Manager → New**. On *Libraries*, add the JAR and pick the class
   `io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver`. On *Settings*: URL template `jdbc:kafcacheql://{host}:{port}`,
   default port `9092`, "No authentication".
3. Create a connection to your broker. The *Driver properties* tab lists the driver's settings with their
   defaults. *Test Connection* reports any setting that's wrong, or the "not built yet" message above.

After rebuilding the JAR, copy it again and restart DBeaver, which keeps the old one loaded.

## Configuration

```
jdbc:kafcacheql://broker1:9092,broker2:9092?config.file=/path/to/client.properties
```

Settings are read in this order, later ones winning: built-in defaults, `config.file`, URL parameters,
the JDBC `Properties` object.

Every setting that isn't one of the driver's own goes unchanged to the Kafka and Schema Registry clients,
so the `client.properties` file Confluent Cloud generates for Java clients works as `config.file`. Keep
credentials out of the URL, which tools show and log: put them in `config.file` or the driver properties.

The driver's own settings (accepted and checked today; most take effect as the stages above land):

| Property | Default | Meaning |
|---|---|---|
| `config.file` | | Properties file loaded before URL parameters and the Properties object |
| `cache.path` | `~/.kafcacheql/<cluster-id>.db` | The SQLite cache file; required when `offline=true` |
| `append.default.window` | `1h` | Window for append-topic queries without an `_offset` or `_timestamp` bound: a duration (`500ms`, `90s`, `15m`, `1h`, `2d`) or `all` |
| `append.default.window.max.records` | `10000` | Per-partition cap on the default window; whichever limit is smaller wins |
| `timestamp.skew.ms` | `60000` | How far past a timestamp to read for `_timestamp < t` on CreateTime topics, for late records |
| `max.staleness.ms` | `0` | Skip the Kafka offsets check when the topic was checked within this time; `0` checks on every query |
| `offline` | `false` | Never contact Kafka or Schema Registry; answer from the cache only |
| `cache.prune.expired` | `true` | Delete cached append-topic data once Kafka has deleted it; `false` keeps it in the cache for as long as you keep the file |
| `topics.include` | `.*` | Regex of topics tools list |
| `topics.exclude` | `__.*\|_confluent.*\|_schemas` | Regex of topics tools don't list; they stay queryable |
| `metadata.show.internal` | `false` | List the cache's internal `$` tables too |
| `client.id` | `kafcacheql` | Client ID; the admin client gets `<client.id>-admin` |
| `sync.batch.records` | `5000` | Records written per SQLite transaction while syncing |

Per topic, `topic.<name>.<setting>` overrides `mode` (`append` or `compacted`), `key.format`, `value.format`,
`append.default.window`, `append.default.window.max.records` and `delete.retention.ms`.

### Authentication

Kafka's own SASL mechanisms (PLAIN, SCRAM, OAUTHBEARER) and TLS are meant to work as in any Kafka client,
Confluent Cloud included; so far SASL/PLAIN is the one tested. Third-party Kafka extensions built against
Kafka's own classes, such as AWS MSK IAM, are not supported: the driver's copy of Kafka is relocated inside
its JAR, where such extensions can't load.

## Other databases

The cache is SQLite, and that's the only engine planned. Other engines, DuckDB first, for large analytical
scans of append topics, may be assessed later. They're not planned yet: a different engine would change the
SQL dialect your queries are written in.

## A word on the cache

The cache is a copy of topic data on your local disk, outside Kafka's access controls. The file is readable
by you only.

Once syncing is built, each query that touches an append topic will also delete the cached records Kafka has
already deleted, whether by time or size retention, so by default the cache keeps no more than the topic does.
To keep data longer than Kafka does, set `cache.prune.expired=false`: the cache then holds everything you've
queried for as long as you keep the file, though only what you queried before Kafka deleted it. Compacted
topics always follow the topic's latest values.

Either way, a topic you stop querying keeps its cached data until you query it again or delete the file. Treat
the cache like any local copy of that data, and check your organisation's rules before caching production
topics.

## Development

The integration tests need the Docker Compose stack in this repository: a Confluent Platform broker (KRaft),
Schema Registry, and Control Center with Prometheus and Alertmanager.

```sh
docker compose up -d broker schema-registry   # enough for the tests; `docker compose up -d` adds Control Center on http://localhost:9021
mvn verify                                    # unit and integration tests
mvn verify -DskipITs                          # unit tests only, no Docker needed
```

## License

[Apache License 2.0](LICENSE)
