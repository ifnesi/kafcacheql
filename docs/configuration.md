# Configuration

Choose which cluster to query, how much data to keep locally, and when to reuse it. A settings file keeps
these choices together; the property reference at the end lists every default.

The **cache** is the driver's local SQLite copy of records fetched from Kafka. Append topics hold event
history; compacted topics hold the latest value per key per partition. A **window** selects the append records
a query needs. Cache limits decide what is kept afterwards. See [query behavior](sql.md#what-a-query-reads)
for how SQL conditions select that window.

## A settings file

Start with one properties file for the connection. It tells the driver where to find schemas and holds any
optional settings. The file can live anywhere, with any name; `config.file` and `cache.path` accept `~/` for
your home directory. This example uses the quick start's Schema Registry and leaves the options commented:

```properties
# Schema Registry, needed for topics with Avro, JSON Schema or Protobuf
schema.registry.url=http://localhost:8081

# Optional driver settings
# cache.path=~/.kafcacheql/local.db
# Opt in to a window; the default is all
# append.default.window=1h
# cache.max.bytes=2000000000
```

Credentials and the authentication mode go in the same file; see [authentication](#authentication). The driver
redacts credential settings from diagnostics, but the file holds them in plain text: keep it readable by you
only.

## The URL

The JDBC URL selects the Kafka brokers and points to your settings file. Use this in your tool's connection
setup, or pass it to JDBC from Java:

```
jdbc:kafcacheql://localhost:9092?config.file=~/.kafcacheql/local.properties
```

This connects to the [quick start](../README.md#quick-start)'s Docker stack. For another cluster, list its
brokers, comma-separated: `jdbc:kafcacheql://broker1:9092,broker2:9092?config.file=~/client.properties`.
URL-encode parameter values containing spaces, `&` or other reserved characters.

Settings are read in this order, later ones winning: built-in defaults, `config.file`, URL parameters, the
JDBC `Properties` object (in DBeaver, the connection's *Driver properties*). The brokers go in one place: the
URL's hosts or `bootstrap.servers`, not both with different values. Settings are read when a connection opens,
so reconnect after changing them.

## Authentication

Credentials and the authentication mode go in the settings file, using the standard Kafka and Schema Registry
client settings. There is no separate driver setting for them. For example, a cluster that uses SASL_SSL with
an API key, and a Schema Registry that uses its own API key:

```properties
# Kafka
bootstrap.servers=broker.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";

# Schema Registry
schema.registry.url=https://registry.example.com
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<SR_API_KEY>:<SR_API_SECRET>
```

- `security.protocol` sets the mode: `PLAINTEXT` (the default), `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`.
- `sasl.mechanism` sets the SASL type: `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`, with the matching login
  module in `sasl.jaas.config`.
- `ssl.*` settings, such as `ssl.truststore.location`, add a trust store or a client certificate when the
  cluster needs one.
- `basic.auth.credentials.source` and `basic.auth.user.info` give Schema Registry its key and secret.

The `client.properties` file Confluent Cloud generates for Java clients uses these same names, so it works as
`config.file` as it is. When the file sets `bootstrap.servers`, leave the URL's host list empty:

```
jdbc:kafcacheql://?config.file=~/.kafcacheql/cloud.properties
```

Keep credentials out of the URL, which tools show and log: put them in `config.file` or the driver properties.

Driver-controlled settings are refused: consumer-group settings such as `group.id` and `enable.auto.commit`
(it reads by offset, with no consumer group), and Schema Registry's `use.latest.version` and `use.schema.id`
(every record is decoded with the schema it names).

Kafka's SASL mechanisms PLAIN and SCRAM, and TLS, are meant to work as in any Kafka client, Confluent Cloud
included; so far SASL/PLAIN is the one tested. OAuth is not yet supported: neither Kafka's `OAUTHBEARER` nor
Schema Registry's bearer tokens (`bearer.auth.*`). Third-party Kafka extensions built against Kafka's own
classes, such as AWS MSK IAM, are not supported: the driver's copy of Kafka is relocated inside its JAR, where
such extensions can't load.

## Offline queries

Use an offline connection to revisit saved data when Kafka or Schema Registry is unavailable. It makes no
calls to either service, so you must fill the cache online first and tell the offline connection which file to
use.

First set `cache.path=~/.kafcacheql/local.db` in your online settings and run the queries you want to keep.
For a second, offline connection, use a separate settings file:

```properties
offline=true
cache.path=~/.kafcacheql/local.db
```

You can also point `cache.path` at an existing cache. The navigator then lists its cached topics, and your SQL
runs against that saved data. Results carry a warning with the cache's as-of time. Missing ranges fail with a
message naming the gaps, rather than returning a partial result.

A configured window duration counts back from each partition's newest cached record; `all` has no time or
record cap.

## Cache lifecycle and sharing

The cache is reusable, but it is not an archive. Online queries remove append records below Kafka's current
log start. A topic you stop querying keeps its cached data unless a cache limit evicts it or a navigator
Refresh discovers that Kafka no longer lists it. Refresh queues that deletion in the background. Evicted data
is fetched again when an online query needs it. Recreated topics and compacted state that may have missed
tombstones (records with null values that delete keys) are rebuilt.

Several tools or applications can share a cache on the same machine. Use a local filesystem, not a network
share or a file shared between the host and a Docker Desktop container. Each query reads its own snapshot, a
consistent view of the cache; SQLite writes run one at a time. Close result sets when finished. An open
snapshot can hold back cleanup of SQLite's write-ahead log, the file recording recent writes, and increase
disk usage. Deleting rows frees pages for reuse but does not shrink the main file.

All users of a cache must agree on `cache.max.bytes`, `cache.retention.hours`, and each topic's `mode`,
`key.format` and `value.format`. In one Java process (JVM), connections must also use the same Kafka and
Schema Registry client settings; incompatible connections are refused. Separate applications must keep
settings that change the cached data aligned themselves. Query settings such as windows, how often to check
Kafka, and listing filters may differ.

If a newer driver refuses an old cache format, close all tools using it and delete that cache. The next online
query rebuilds it from Kafka.

## Cache security

The cache is an unencrypted local copy outside Kafka's access controls. Revoking Kafka access does not erase
it. On POSIX filesystems the driver restricts the cache files to their owner; on other filesystems, set
equivalent permissions yourself. Protect the cache and settings file as you would any copy of the data and
credentials.

`topics.include` and `topics.exclude` only filter navigator listings. They do not prevent queries or erase
excluded topics from the cache.

## Per-topic settings

Use these when one topic needs different decoding or query limits from the rest of the connection. `mode`
selects append history or compacted state; the formats describe how to decode each key and value.
`delete.retention.ms` is how long Kafka retains tombstones, the deletion markers for compacted keys.

Use `topic.<name>.<setting>` to override `mode` (`append` or `compacted`), `key.format` and `value.format`
(`auto`, the default, `avro`, `json-sr`, `protobuf`, `json`, `string` or `bytes`), `append.default.window`,
`append.default.window.max.records` and `delete.retention.ms`. The last is only used when the topic's own
config can't be read (no DescribeConfigs permission); otherwise the broker's value wins. Without that
permission, `mode` is required. For a compacted topic, also set `delete.retention.ms` to the broker's value;
the fallback is 86400000 (24 hours).

## The driver's own settings

The complete property reference is below. Settings not supplied use these defaults.

| Property | Default | Meaning |
|---|---|---|
| `config.file` | | Properties file loaded before URL parameters and the Properties object |
| `cache.path` | `~/.kafcacheql/<cluster-id>.db` | The SQLite cache file; required when `offline=true` |
| `cache.max.bytes` | none | Positive limit on SQLite pages in use, not file length. Evicts oldest append records first, then whole compacted topics, least recently queried first. Eviction is queued after the query has its reader snapshot, so the cache can temporarily exceed the limit |
| `cache.retention.hours` | none | Positive whole hours to keep append records, by record timestamp, whatever Kafka's retention. Compacted topics are excluded. A query can fetch older data again; eviction follows its reader snapshot |
| `append.default.window` | `all` | Window for append-topic queries without an `_offset` or `_timestamp` bound: a duration (`500ms`, `90s`, `15m`, `1h`, `2d`) or `all` (the whole topic, with no record limit either) |
| `append.default.window.max.records` | `10000` | Per-partition cap when a window duration is set; whichever limit is smaller wins. Ignored for `all`. |
| `max.staleness.ms` | `0` | Skip topic description and start/end offset lookups within this many milliseconds of this JVM's last valid check. `0` checks on every query. Timestamp bounds still ask Kafka for their offsets |
| `offline` | `false` | Answer from an existing cache without contacting Kafka or Schema Registry; requires `cache.path`. See [offline queries](#offline-queries) |
| `topics.include` | `.*` | Regex of topics tools list (a listing filter, not access control) |
| `topics.exclude` | `__.*\|_confluent.*\|_schemas` | Regex of topics tools don't list; they stay queryable |
| `metadata.show.internal` | `false` | List the cache's internal `$` tables too |
| `client.id` | `kafcacheql` | Base client ID; the admin uses `<client.id>-admin`. Numeric suffixes avoid duplicate IDs within a JVM |
| `sync.batch.records` | `5000` | Maximum records per SQLite transaction while syncing |
| `sync.fetch.parallelism` | `4` | Maximum partitions fetched and decoded at once, each by its own consumer. SQLite writes remain sequential; Schema Registry is asked once per schema ID. `1` fetches one partition at a time |

See [what a query reads](sql.md#what-a-query-reads) for bounds, default windows and tail queries.
