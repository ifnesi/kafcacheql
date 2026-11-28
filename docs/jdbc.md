# JDBC behavior and permissions

Use this page to understand what a navigator refresh or SQL query asks of Kafka, and which permissions the
connection needs. KafCacheQL keeps fetched records in a local SQLite cache. Browsing tables and fetching their
data are separate operations.

## From connection to results

1. **Connect:** the driver identifies the Kafka cluster and opens its cache. See the
   [connection-opening timeout](configuration.md#the-url) if the brokers cannot be reached.
2. **Browse:** the tool asks for metadata, meaning topic names and column descriptions. These calls never
   fetch topic records. Before a topic is cached, only its built-in columns are known.
3. **Query:** the driver works out the query's window, the range of records it needs, and fetches what is
   missing. SQLite then runs the SQL on a consistent view of the cache.

Schema Registry is opened only when a record names a schema. Its ID or GUID identifies how to decode that
record. Columns come from cached schemas, without subject or latest-version lookups.

An **offline connection** answers from the cache without Kafka or Schema Registry. Online, `max.staleness.ms`
can reuse recent topic checks; it does not prevent fetching missing records. See
[configuration](configuration.md) for both options.

## While a query runs

Connections are read-only, and results are forward-only: callers read rows in sequence. `Statement.cancel()`
and `setQueryTimeout()` cover both fetching data and SQL execution. Long syncs log progress per partition
about every five seconds.

Close results and statements when finished. This releases the query's reader snapshot, its consistent view of
the cache, so SQLite can clean up old writes. See [cache
lifecycle](configuration.md#cache-lifecycle-and-sharing).

## Calls and permissions

This table maps tool actions to JDBC methods, network calls and cache changes. For tool setup, see [DBeaver
and DataGrip](tools.md).

JDBC metadata reports the product as KafCacheQL and the database version as the driver's version, so tools
should use Generic SQL. Unsupported function and pseudo-column listings return empty results with standard
JDBC columns.

| JDBC call (in DBeaver, for example) | Kafka and Schema Registry calls | In the cache | Topic permission |
|---|---|---|---|
| Connect | Looks up the cluster ID when opening shared Kafka clients | Opens or creates the cache and checks its cluster ID | |
| `getTables()`: the navigator, and its Refresh | Lists topics on every call | Queues background drops for cached topics Kafka no longer lists | Describe; topics without it aren't listed |
| `getColumns()`: expanding a table, autocomplete | None | Reads columns for topics in the latest listing or cache; uncached topics have only built-in columns | |
| First `SELECT` on a topic (DBeaver's double-click runs `SELECT *`) | Lists topics to resolve an uncached name; reads config, description and offsets, then fetches the query's window. Schema Registry loads each record's schema ID or GUID and its fixed-version references as needed | Creates the topic's table and view and fills them | Describe and Read; DescribeConfigs or [per-topic settings](configuration.md#per-topic-settings) |
| Later `SELECT`s | Topic description and start/end offsets, then missing records only. Config is cached per connection by topic ID | Updates rows and view columns | Describe and Read |
| `SELECT` within `max.staleness.ms` of the last valid check | Skips description and start/end lookups. Missing ranges still need fetching; timestamp bounds still need offset lookups | Reuses the recent topic observation | As for `SELECT` |
| `SHOW TABLES` | As `getTables()`. Offline, none | As `getTables()`: queues background drops for cached topics Kafka no longer lists | As `getTables()` |
| `DESCRIBE` | Lists topics when an online name isn't cached; otherwise none | Reads cached columns | As `getTables()` |
| `DROP TABLE` | None | Deletes the topic's cached records and offsets; Kafka is unchanged | |
| `SYNC` | Lists topics, then as a later `SELECT`, fetching everything new | Adds rows; holds a lock file beside the cache while it runs | Describe and Read |
| Any call with `offline=true` | None | Answers from the cache only | |
| `getPrimaryKeys()` | A named uncached topic: description and config. Null or a listing pattern: lists topics, then one batched config lookup for uncached modes | Reads cached modes; creates no tables | Describe; DescribeConfigs or a mode override |
| `getIndexInfo()` and the rest of `DatabaseMetaData` | None | | |
