# KafCacheQL

*Kafka + Cache + Query Language*

> **Work in progress.** This driver is in early development (see [Status](#status)). Queries run through
> JDBC, from a tool's SQL editor or JVM code, and tools list topics and columns in their navigators.
> Nothing here is stable: names, settings and behaviour may change. Not for production use.

**What if you could query Kafka topics from your database tool of choice? No Kafka client, just a JDBC
driver. And best of all, whatever you query is cached locally, automatically.**

KafCacheQL is a read-only JDBC driver for Apache Kafka. Point DBeaver, DataGrip or any JVM application at a
Kafka cluster and query topics with plain SQL:

```sql
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
- Working with a topic's data offline, from the cache (`offline=true`).

## What it isn't

- **Not real-time.** There are no continuous queries and no streaming results. A query answers from the cache
  as it stands once the query's data has been fetched; to see newer records, run the query again.
- **Not for production systems.** It's a tool for people exploring data, not a component to build services on.
- **Not a Kafka consumer replacement.** It has no consumer group and no processing guarantees. Applications
  that process events should use the Kafka clients or Kafka Streams.
- **Read-only.** SELECT only: no INSERT, UPDATE or DELETE, and it never produces to Kafka.
- **Not a database server.** Nothing runs besides your tool and Kafka (plus Schema Registry, when topics use it).

## Quick start with Docker

This runs everything on your machine: the Confluent Platform stack in this repository (a Kafka broker and
Schema Registry), two sample topics, and DBeaver.

Requirements: Docker, Java 21 or newer and Maven 3.9 or newer to build the driver, and
[DBeaver](https://dbeaver.io/). The driver works with Kafka 3.1 or newer (Confluent Platform 7.1 or newer).

### 1. Start Kafka and Schema Registry

```sh
docker compose up -d broker schema-registry
```

The broker listens on `localhost:9092` and Schema Registry on `http://localhost:8081`.
`docker compose up -d` also starts Control Center on <http://localhost:9021>.

### 2. Build the driver

```sh
mvn package -DskipTests
```

This builds `target/kafcacheql-jdbc-0.1.0-SNAPSHOT.jar`, one self-contained JAR (about 40 MB) with Kafka's
and Confluent's libraries relocated inside it, so it doesn't clash with other versions in the same
application. It carries native libraries for Linux, macOS and Windows on x86-64 and ARM64 only. Measured
timings for large topics are in [docs/performance.md](docs/performance.md). Copy it somewhere stable: `mvn clean` deletes `target/`.

### 3. Create two sample topics

A compacted `customers` topic and an append `orders` topic, with Avro values in Schema Registry. The
console producer runs inside the Schema Registry container; each line is `key|value`, and the key is JSON
too, hence the quotes.

```sh
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic customers --partitions 1 --config cleanup.policy=compact
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic orders --partitions 3

printf '%s\n' '"c1"|{"name":"Ada","country":"UK"}' '"c2"|{"name":"Linus","country":"FI"}' \
  '"c3"|{"name":"Grace","country":"US"}' |
docker compose exec -T schema-registry kafka-avro-console-producer \
  --bootstrap-server broker:29092 --topic customers \
  --reader-property schema.registry.url=http://localhost:8081 \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  --reader-property 'key.schema={"type":"string"}' \
  --reader-property 'value.schema={"type":"record","name":"Customer","fields":[{"name":"name","type":"string"},{"name":"country","type":"string"}]}'

printf '%s\n' '"o1"|{"order_id":"o1","customer_id":"c1","status":"PAID","amount":12.5}' \
  '"o2"|{"order_id":"o2","customer_id":"c2","status":"NEW","amount":7.0}' \
  '"o3"|{"order_id":"o3","customer_id":"c1","status":"PAID","amount":3.25}' \
  '"o4"|{"order_id":"o4","customer_id":"c3","status":"SHIPPED","amount":20.0}' |
docker compose exec -T schema-registry kafka-avro-console-producer \
  --bootstrap-server broker:29092 --topic orders \
  --reader-property schema.registry.url=http://localhost:8081 \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  --reader-property 'key.schema={"type":"string"}' \
  --reader-property 'value.schema={"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"customer_id","type":"string"},{"name":"status","type":"string"},{"name":"amount","type":"double"}]}'
```

### 4. Write a settings file

Create `~/.kafcacheql/local.properties` (`mkdir -p ~/.kafcacheql` first):

```properties
schema.registry.url=http://localhost:8081
```

See [Configuration](#configuration) for everything else it can hold.

### 5. Add the driver to DBeaver

1. **Database → Driver Manager → New**, with *Driver Type* **Generic** (not a copy of another database's
   driver, or DBeaver runs that database's own queries on connect).
2. *Libraries*: add the JAR, then pick the class `io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver`
   (*Find Class* lists it).
3. *Settings*: URL template `jdbc:kafcacheql://{host}:{port}?config.file={file}`, default port `9092`,
   "No authentication". With `{file}` in the template, the connection page shows a *Path* field for the
   settings file. If your DBeaver version doesn't show it, use `jdbc:kafcacheql://{host}:{port}` and add a
   user property `config.file` under the connection's *Driver properties* instead.

After rebuilding the JAR, copy it again and restart DBeaver, which keeps the old one loaded.

### 6. Connect and query

1. Create a connection with the driver: host `localhost`, port `9092`, and the path of the settings file
   (`~/.kafcacheql/local.properties`). *Test Connection*.
2. Open the connection: the navigator lists the topics you can access, `customers` and `orders` among
   them. Topics you haven't queried yet show only the built-in columns; their schema fields appear after
   the first query, once you refresh the navigator.
3. Open an SQL editor and try:

```sql
-- Orders per status in the last 15 minutes: Kafka is read from the first record of that time onwards
SELECT status, count(*) FROM "orders"
WHERE _timestamp >= unixepoch('now', '-15 minutes') * 1000
GROUP BY status;

-- Join the orders with the latest state of each customer
SELECT o.order_id, o.amount, c.name, c.country
FROM "orders" AS o JOIN "customers" AS c ON c.key = o.customer_id
WHERE o._timestamp >= unixepoch('now', '-1 day') * 1000;
```

The cache file is `~/.kafcacheql/<cluster-id>.db`. Run the same query again and only new records are
fetched.

## How it works

![KafCacheQL architecture: the driver runs inside your tool, syncs records from Kafka into a local SQLite cache, and runs your SQL there](docs/architecture.svg)

1. A query names topics as tables: `"orders"`, `"payments.v1"`.
2. For each topic, the driver works out which offsets the query needs. Compacted topics are read up to their
   end; append topics are read for the window the query's `WHERE` clause asks for on `_partition`, `_offset`
   or `_timestamp`, or a default window (the last hour, at most 10,000 records per partition) when it doesn't
   say.
3. Only the offsets not yet cached are fetched, then decoded (Avro and JSON Schema with Schema Registry, plain
   JSON, strings and bytes; Protobuf later) and stored as JSON in SQLite, with each record's
   `_partition`, `_offset`, `_timestamp`, `_key` and `_headers`.
4. SQLite runs the query. Each topic's table is a view with a column per field of the schemas its cached
   records were written with.

Compacted topics hold the latest value per key in each partition, as Kafka does, and tombstones delete keys.
When the cache could have missed deletions, or a topic was recreated, the topic is rebuilt.

## SQL examples

The examples on `orders` and `customers` run on the [quick start](#quick-start-with-docker)'s data; the last
two show syntax on topics you'd have yourself.

Queries are SQLite's SQL. The driver reads each query with a SQL parser (JSqlParser) first, to find its
topics and bounds, so a few SQLite forms it can't parse are refused: `GLOB`, `BETWEEN … IS TRUE` or
`IS NULL` without brackets, and a column written with the schema name in front (`main."orders"._offset`;
use `"orders"._offset` or an alias). Only SELECT statements are accepted.

Each topic is a view named after the topic (a topic starting with `sqlite_`, a prefix SQLite reserves, gets
`sqlite-` instead, and a second topic differing only in letter case gets a `$2` suffix, `"Orders$2"`: use the name the navigator
shows). Every view has the record's `_partition`, `_offset`,
`_timestamp` (epoch milliseconds), `_key` and `_value` (decoded, as JSON), `_headers` (a JSON array of
`{"key": …, "value": …}`, values in base64), `_key_schema_id` and `_value_schema_id` (the schema each record
names) and `_decode_error`, plus a column per top-level field of the schemas in its cached records (for plain JSON without a schema,
of the top-level keys in its newest 1,000 cached records; a key whose type varies becomes a text column, with
a warning). A
primitive key with a schema is also a `key` column; a key without one is in `_key` (`_key ->> '$'`). Nested values stay JSON; Avro decimals have numeric columns.

```sql
-- Orders per status in the last 15 minutes: Kafka is read from the first record of that time onwards
SELECT status, count(*)
FROM "orders"
WHERE _timestamp >= unixepoch('now', '-15 minutes') * 1000
GROUP BY status;

-- An exact offset range in one partition
SELECT _offset, order_id, amount
FROM "orders"
WHERE _partition = 0 AND _offset BETWEEN 0 AND 10;

-- No _offset or _timestamp bound: the default window applies (the last hour, at most 10,000 records per
-- partition) and the driver adds a warning saying so
SELECT * FROM "orders" ORDER BY _timestamp DESC LIMIT 100;

-- Join an append topic with a compacted reference topic (always read to its end)
SELECT o.order_id, o.amount, c.name, c.country
FROM "orders" AS o
JOIN "customers" AS c ON c.key = o.customer_id
WHERE o._timestamp >= unixepoch('now', '-1 day') * 1000;

-- Any field through JSON, even one without a column yet
SELECT _value ->> '$.status' AS status, _key FROM "orders" WHERE _offset >= 0;

-- The latest orders: a tail query fetches only the last 5 records of each partition, with no default window
SELECT order_id, status, _partition, _offset FROM "orders" ORDER BY _offset DESC LIMIT 5;

-- Syntax only: nested fields, and records carrying a given header
SELECT o._value ->> '$.address.city' AS city
FROM "shipments" AS o, json_each(o._headers) AS h
WHERE o._timestamp >= unixepoch('now', '-1 hour') * 1000
  AND h.value ->> '$.key' = 'trace-id';

-- Syntax only: topic names with dots or dashes are quoted
SELECT count(*) FROM "payments.v1" WHERE _offset >= 0;
```

### Plain JSON and JSON Schema topics

Topics without Schema Registry get columns too, from the top-level keys of their newest 1,000 cached records.
A plain JSON `events` topic:

```sh
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic events --partitions 1
printf '%s\n' '{"type":"click","page":"/home","ms":120}' '{"type":"view","page":"/cart","ms":80}' \
  '{"type":"click","page":"/cart","ms":95}' |
docker compose exec -T broker kafka-console-producer --bootstrap-server broker:29092 --topic events
```

```sql
SELECT type, count(*) AS n, avg(ms) AS avg_ms FROM "events" WHERE _offset >= 0 GROUP BY type;
```

Topics with JSON Schema in Schema Registry work like Avro ones: a column per property of the schemas in the
cached records. A `payments` topic:

```sh
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic payments --partitions 1
printf '%s\n' '{"payment_id":"p1","order_id":"o1","amount":12.5,"method":"card"}' \
  '{"payment_id":"p2","order_id":"o3","amount":3.25,"method":"cash"}' |
docker compose exec -T schema-registry kafka-json-schema-console-producer \
  --bootstrap-server broker:29092 --topic payments \
  --reader-property schema.registry.url=http://localhost:8081 \
  --reader-property 'value.schema={"type":"object","properties":{"payment_id":{"type":"string"},"order_id":{"type":"string"},"amount":{"type":"number"},"method":{"type":"string"}}}'
```

```sql
SELECT p.payment_id, p.method, p.amount, o.status
FROM "payments" AS p JOIN "orders" AS o ON o.order_id = p.order_id
WHERE p._offset >= 0 AND o._offset >= 0;
```

### Offline, and fewer Kafka checks

With `offline=true` the driver never contacts Kafka or Schema Registry and answers from the cache file
alone, so `cache.path` must name it. Add a second connection with its own settings file, for example
`~/.kafcacheql/offline.properties`:

```properties
offline=true
cache.path=~/.kafcacheql/local.db
```

and set `cache.path=~/.kafcacheql/local.db` in `local.properties` too, so both use the same file. Offline,
the same queries answer from what's cached: the default window counts back from each partition's newest
cached record, results carry a warning with the cache's as-of time, and a range that isn't cached fails,
naming it. The navigator lists only cached topics.

Online, `max.staleness.ms=60000` in a settings file skips the Kafka checks (topic description and offsets)
for a topic checked in the last minute, so repeated queries answer straight from the cache; timestamp
bounds still ask Kafka for their offsets.

### Compacted topics and Kafka calls

Compacted topics hold one row per key in each partition, as Kafka does. If partitions were added to a topic,
a key can appear in more than one; pick the newest with `_timestamp`.

Each query checks the topic ID, partition count and offsets with Kafka; a connection reads each topic's
configuration once. During long syncs, progress is logged every five seconds per partition.

## Status

| Stage | State |
|---|---|
| Build, shaded JAR, test environment | Done |
| Driver, URL and configuration | Done |
| Client settings, credentials kept out of logs and errors | Done |
| Kafka clients: cluster ID, topic metadata, offsets | Done |
| SQLite cache file and its internal tables | Done |
| Topic tables, decoding, syncing records into the cache | Done |
| Cache validity and rebuilds, SQL analysis, windows, query rewrite | Done |
| Typed views from cached schema IDs | Done |
| Whole-query pipeline, incremental fetches and progress | Done |
| Connections, statements, result sets, column types from the schema | Done |
| Database metadata in tools (navigator, autocomplete) | Done |
| Checked by hand in DBeaver and DataGrip | Next |
| Offline mode and `max.staleness.ms` | Done |
| Columns for plain JSON topics, inferred from cached records | Done |
| JSON Schema with Schema Registry | Done |
| Protobuf | Later |

## Configuration

```
jdbc:kafcacheql://localhost:9092?config.file=~/.kafcacheql/local.properties
```

That's the quick start's Docker stack. For a cluster, list one or more of its brokers, comma-separated:
`jdbc:kafcacheql://broker1:9092,broker2:9092?config.file=/path/to/client.properties`.

Settings are read in this order, later ones winning: built-in defaults, `config.file`, URL parameters,
the JDBC `Properties` object. The brokers go in one place: the URL's hosts or `bootstrap.servers`, not
both with different values.

Every setting that isn't one of the driver's own goes unchanged to the Kafka and Schema Registry clients,
so the `client.properties` file Confluent Cloud generates for Java clients works as `config.file`. A few
are refused, because the driver controls them: consumer-group settings such as `group.id` and
`enable.auto.commit` (it reads by offset, with no consumer group), and Schema Registry's
`use.latest.version` and `use.schema.id` (every record is decoded with the schema it names). Keep
credentials out of the URL, which tools show and log: put them in `config.file` or the driver properties.

### A settings file

Keeping every setting in one file is the simplest way to configure a connection. The file can live
anywhere, with any name; `config.file` and `cache.path` accept `~/` for your home directory.

1. Create a file, for example `~/.kafcacheql/local.properties` for the quick start's Docker stack
   (`mkdir -p ~/.kafcacheql` first):

```properties
# Schema Registry, needed for topics with Avro and Schema Registry
schema.registry.url=http://localhost:8081

# Optional driver settings
# cache.path=~/.kafcacheql/local.db
# append.default.window=1h
```

   Any Kafka or Schema Registry client setting can go in it too (`security.protocol`, `sasl.*`, `ssl.*`,
   `basic.auth.user.info` and so on). Credentials in it never appear in logs or errors.
2. Point the connection at it, in either of two ways:
   - in DBeaver, on the connection's *Driver properties* tab, add a user property `config.file` with the
     file's path;
   - or in the URL: `jdbc:kafcacheql://localhost:9092?config.file=~/.kafcacheql/local.properties`.
3. Reconnect: settings are read when a connection opens.

Settings in *Driver properties* or the URL override the file's.

The driver's own settings:

| Property | Default | Meaning |
|---|---|---|
| `config.file` | | Properties file loaded before URL parameters and the Properties object |
| `cache.path` | `~/.kafcacheql/<cluster-id>.db` | The SQLite cache file; required when `offline=true` |
| `append.default.window` | `1h` | Window for append-topic queries without an `_offset` or `_timestamp` bound: a duration (`500ms`, `90s`, `15m`, `1h`, `2d`) or `all` (the whole topic, with no record limit either) |
| `append.default.window.max.records` | `10000` | Per-partition cap on the default window; whichever limit is smaller wins |
| `max.staleness.ms` | `0` | Skip the Kafka offsets check when the topic was checked within this time; `0` checks on every query. Timestamp bounds still ask Kafka for their offsets |
| `offline` | `false` | Never contact Kafka or Schema Registry; answer from the cache only; requires `cache.path`. The default window counts back from each partition's newest cached record, a range that isn't cached fails naming it, and results carry a warning with the cache's as-of time |
| `topics.include` | `.*` | Regex of topics tools list |
| `topics.exclude` | `__.*\|_confluent.*\|_schemas` | Regex of topics tools don't list; they stay queryable |
| `metadata.show.internal` | `false` | List the cache's internal `$` tables too |
| `client.id` | `kafcacheql` | Client ID; the admin client gets `<client.id>-admin` |
| `sync.batch.records` | `5000` | Records written per SQLite transaction while syncing |

Per topic, `topic.<name>.<setting>` overrides `mode` (`append` or `compacted`), `key.format`, `value.format`,
`append.default.window`, `append.default.window.max.records` and `delete.retention.ms`. The last is only used
when the topic's own config can't be read (no DescribeConfigs permission); otherwise the broker's value wins.

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

Each query that touches an append topic also deletes the cached records Kafka has
already deleted, whether by time or size retention, so the cache keeps no more than the topic does.
Compacted topics always follow the topic's latest values.

A topic you stop querying keeps its cached data until you query it again or delete the file. Treat
the cache like any local copy of that data, and check your organisation's rules before caching production
topics.

Several tools or applications can share one cache file, as long as they run on the same machine (not on a
network share, nor shared between your host and a Docker Desktop container) and use the same settings that
shape the cache: each topic's `mode` and formats. Within one application, a
connection with different ones is refused; separate applications sharing a file must keep them the same
themselves.

## Development

The integration tests need the Docker Compose stack in this repository: a Confluent Platform broker (KRaft)
on `localhost:9092` (and SASL/PLAIN on `localhost:9094`, user `test`, password `test-secret`), Schema
Registry on `http://localhost:8081`, and Control Center with Prometheus and Alertmanager on
<http://localhost:9021>. The tests create topics with unique names and leave them behind.

```sh
docker compose up -d broker schema-registry   # enough for the tests; `docker compose up -d` adds Control Center on http://localhost:9021
mvn verify                                    # unit and integration tests
mvn verify -DskipITs                          # unit tests only, no Docker needed
```

Some tests log warnings on purpose, and they all print. Tests that recreate topics, expire tombstones or
change settings log `Rebuilding topic …`, and Kafka's consumer logs `inconsistent topic ID` /
`unknown topic or partition` for a recreated topic. Tests of unreachable brokers or Schema Registry log
connection errors to `localhost:1` or to a closed port, and the wrong-password test logs Kafka's
`failed authentication`. Tests of misspelt settings log `isn't a KafCacheQL
property`. A test fails only on its own assertions.

## License

[Apache License 2.0](LICENSE)
