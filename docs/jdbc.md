# What each JDBC call does in Kafka

What a tool's actions cost on the Kafka and Schema Registry side, and the Kafka topic permissions they need.

| JDBC call (in DBeaver, for example) | Kafka and Schema Registry calls | In the cache | Topic permission |
|---|---|---|---|
| Connect | The cluster ID, once | Opens the cache file, or creates it, and checks it belongs to that cluster | |
| `getTables()`: the navigator, and its Refresh | Lists topics, on every call | Drops the cached copy of topics Kafka no longer lists, in the background | Describe; topics without it aren't listed |
| `getColumns()`: expanding a table, autocomplete | None | Reads the columns from the topic's view: before its first query, only the built-in ones | |
| First `SELECT` on a topic (DBeaver's double-click runs `SELECT *`) | The topic's config (once per connection), its description and offsets, then fetches the query's window. Schema Registry: each schema ID once, the first time a record names it | Creates the topic's table and view and fills them | Describe and Read; DescribeConfigs for the config, else `topic.<name>.delete.retention.ms` |
| Later `SELECT`s | The topic's description and offsets, then fetches only the records not yet cached; with nothing new, nothing more | Adds rows; new schema IDs add columns | Describe and Read |
| `SELECT` within `max.staleness.ms` of the last check | None, unless a `_timestamp` bound needs its offsets looked up | | |
| Any call with `offline=true` | None | Answers from the cache only | |
| `getPrimaryKeys()`, `getIndexInfo()` and the rest of `DatabaseMetaData` | None | | |

The driver never produces to Kafka and never commits consumer offsets: it has no consumer group.
