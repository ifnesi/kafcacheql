# Configuration

## The URL

```
jdbc:kafcacheql://localhost:9092?config.file=~/.kafcacheql/local.properties
```

That's the quick start's Docker stack. For a cluster, list one or more of its brokers, comma-separated:
`jdbc:kafcacheql://broker1:9092,broker2:9092?config.file=/path/to/client.properties`.

Settings are read in this order, later ones winning: built-in defaults, `config.file`, URL parameters,
the JDBC `Properties` object (in DBeaver, the connection's *Driver properties*). The brokers go in one place:
the URL's hosts or `bootstrap.servers`, not both with different values. Settings are read when a connection
opens, so reconnect after changing them.

Every setting that isn't one of the driver's own goes unchanged to the Kafka and Schema Registry clients,
so the `client.properties` file Confluent Cloud generates for Java clients works as `config.file`. A few
are refused, because the driver controls them: consumer-group settings such as `group.id` and
`enable.auto.commit` (it reads by offset, with no consumer group), and Schema Registry's
`use.latest.version` and `use.schema.id` (every record is decoded with the schema it names). Keep
credentials out of the URL, which tools show and log: put them in `config.file` or the driver properties.

## A settings file

Keeping every setting in one file is the simplest way to configure a connection. The file can live
anywhere, with any name; `config.file` and `cache.path` accept `~/` for your home directory.

```properties
# Schema Registry, needed for topics with Avro or JSON Schema
schema.registry.url=http://localhost:8081

# Optional driver settings
# cache.path=~/.kafcacheql/local.db
# append.default.window=1h
# cache.max.bytes=2000000000
```

Any Kafka or Schema Registry client setting can go in it too (`security.protocol`, `sasl.*`, `ssl.*`,
`basic.auth.user.info` and so on). Credentials in it never appear in logs or errors, but the file itself
holds them in plain text: keep it readable by you only.

## The driver's own settings

| Property | Default | Meaning |
|---|---|---|
| `config.file` | | Properties file loaded before URL parameters and the Properties object |
| `cache.path` | `~/.kafcacheql/<cluster-id>.db` | The SQLite cache file; required when `offline=true` |
| `cache.max.bytes` | none | Limit on the space the cache uses, in bytes (SQLite's pages in use, not the file's length, which doesn't shrink). Over it, the oldest cached records of append topics are deleted first, across topics; if that isn't enough, whole compacted topics, least recently queried first. A later query fetches them again. Checked after a query's sync, so the cache can be over the limit while a query runs |
| `cache.retention.hours` | none | Delete cached append-topic records older than this many hours, by record timestamp, whatever Kafka's retention. Compacted topics are excluded. A query for older data fetches it again, and it's deleted afterwards |
| `append.default.window` | `1h` | Window for append-topic queries without an `_offset` or `_timestamp` bound: a duration (`500ms`, `90s`, `15m`, `1h`, `2d`) or `all` (the whole topic, with no record limit either) |
| `append.default.window.max.records` | `10000` | Per-partition cap on the default window; whichever limit is smaller wins |
| `max.staleness.ms` | `0` | Skip the Kafka offsets check when the topic was checked within this time; `0` checks on every query. Timestamp bounds still ask Kafka for their offsets |
| `offline` | `false` | Never contact Kafka or Schema Registry; answer from the cache only; requires `cache.path`. The default window counts back from each partition's newest cached record, a range that isn't cached fails naming it, and results carry a warning with the cache's as-of time |
| `topics.include` | `.*` | Regex of topics tools list (a listing filter, not access control) |
| `topics.exclude` | `__.*\|_confluent.*\|_schemas` | Regex of topics tools don't list; they stay queryable |
| `metadata.show.internal` | `false` | List the cache's internal `$` tables too |
| `client.id` | `kafcacheql` | Client ID; the admin client gets `<client.id>-admin` |
| `sync.batch.records` | `5000` | Records written per SQLite transaction while syncing |

Per topic, `topic.<name>.<setting>` overrides `mode` (`append` or `compacted`), `key.format`, `value.format`,
`append.default.window`, `append.default.window.max.records` and `delete.retention.ms`. The last is only used
when the topic's own config can't be read (no DescribeConfigs permission); otherwise the broker's value wins.

Settings that shape the cache must match for everything sharing one cache file: `cache.max.bytes`,
`cache.retention.hours`, and each topic's `mode`, `key.format` and `value.format`. Within one application, a
connection with different ones is refused; separate applications sharing a file must keep them the same
themselves. Settings that only shape queries (windows, staleness, topic filters) may differ.

## Authentication

Kafka's own SASL mechanisms (PLAIN, SCRAM, OAUTHBEARER) and TLS are meant to work as in any Kafka client,
Confluent Cloud included; so far SASL/PLAIN is the one tested. Third-party Kafka extensions built against
Kafka's own classes, such as AWS MSK IAM, are not supported: the driver's copy of Kafka is relocated inside
its JAR, where such extensions can't load.
