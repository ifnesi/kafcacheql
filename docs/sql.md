# SQL reference

Use SQL to join topic data, summarize events and inspect individual records. KafCacheQL runs SELECTs on a
local SQLite cache, the records it has fetched from Kafka. Joins, aggregations, window functions, CTEs and
JSON functions use SQLite SQL. Both SELECT and `WITH … SELECT` are accepted, plus a few [cache
statements](#cache-statements) for looking at and managing the cache.

## Tables

Each topic appears as a table. Internally it is a SQLite view over the cached records. Its rows follow the
topic's mode:

- **Append:** individual records form an event history, including records that share a key. Records with null
  values, called tombstones, remain as rows.
- **Compacted:** the latest value per key per partition forms the current state. A tombstone removes that
  key's row. The same key can appear in several partitions.

Kafka reads use `read_committed`. Use the table name shown in your tool's navigator; quote names containing
dots or dashes, such as `"payments.v1"`.

## Columns

Rows expose the decoded Kafka key and value as JSON, in `_key` and `_value`, plus the record's partition,
offset, timestamp and headers. When a record names a schema, its schema ID or GUID identifies the
Schema Registry schema used to decode it.

| Column | Meaning |
|---|---|
| `_partition`, `_offset` | Where the record is in Kafka |
| `_timestamp` | Record timestamp, epoch milliseconds |
| `_key`, `_value` | Decoded key and value, as JSON |
| `_headers` | JSON array of headers in record order; see [Headers](#headers) |
| `_key_schema_id`, `_value_schema_id` | Schema ID or GUID named by each record, when present |
| `_decode_error` | A decoding or transform failure; NULL when neither failed |

Without transforms, a part that fails to decode is kept as a base64 JSON string. With
[record transforms](transforms.md#what-queries-see), payload columns contain only the transformed data;
failed rows keep no key, value or headers. The transform guide explains these failure rules.

Top-level value fields become columns too. Avro, JSON Schema and Protobuf columns come from the schemas named
by cached records. Plain JSON columns are inferred from cached data. A primitive key with a schema becomes
`key`; record key fields get a `key_` prefix. A primitive key without a schema stays in `_key`; read it with
`_key ->> '$'`.

Nested values stay JSON. Read a nested field with `->>`, for example `_value ->> '$.address.city'`. See
[column type details](#column-type-details) for mixed record types, schema changes and conversions.

### Headers

`_headers` is a JSON array with one object per header, in record order, including duplicate names. A text
value, valid UTF-8, is shown as text under `value`. Any other bytes are base64 under `base64`. A null value
is `"value": null`, and a record without headers has `[]`:

```json
[{"key": "trace-id", "value": "4bf92f35"}, {"key": "flags", "base64": "AAH/"}, {"key": "empty", "value": ""}]
```

Read one header as text:

```sql
SELECT h.value ->> '$.value' AS trace_id
FROM "shipments" AS o, json_each(o._headers) AS h
WHERE h.value ->> '$.key' = 'trace-id';
```

## What a query reads

A query's **window** is the range of records it needs from each append topic. By default it includes
everything Kafka still holds. Conditions on `_partition`, `_offset` and `_timestamp` can narrow it. The driver
fetches missing ranges, then SQLite applies the complete query to a consistent cached snapshot.

Compacted topics are read to their full current state before SQL filters the rows. An **offline** connection
uses only the cache, without contacting Kafka or Schema Registry; missing ranges fail the query.

A plain `LIMIT` limits returned rows, not the initial sync. A tail query, ordered by `_offset DESC` with a
literal limit and only optional partition conditions, can fetch the last records instead. Offsets are numbered
per partition, so this does not mean the newest records by time. See [window details](#window-details) for the
exact rules and optional time windows.

## Cache statements

These statements act on the local cache only. None of them changes anything in Kafka.

| Statement | Result |
|---|---|
| `SHOW TABLES` | One row per topic: `table_name`, `mode`, `partitions`, `cached_rows`, `cached_bytes`. The last four are NULL for a topic with nothing cached |
| `DESCRIBE "orders"` | One row per column: `column_name`, `type`. Before a topic's first query, only the built-in columns |
| `DROP TABLE "orders"` | Deletes the topic's cached records and offsets. **The Kafka topic is not deleted.** The next online query fetches it again |
| `SYNC "orders", "customers"` | Brings topics up to date, and returns `table_name` and `fetched_records` for each |

`SHOW TABLES` lists the same topics as your tool's navigator; offline, it lists the cached ones. Counting rows
and bytes reads the cache, so it takes longer on a large cache. `DROP TABLE IF EXISTS` doesn't fail when the
topic has nothing cached.

### SYNC or SELECT

`SYNC` follows one rule: it fetches everything new, uncapped. For each partition it reads from the newest
cached offset to Kafka's current end; a topic with nothing cached is read in full. A configured window,
record cap or `WHERE` clause plays no part. To fetch less, use a `SELECT`: its window and cap apply even to
`SELECT *`. `SYNC` doesn't fill older gaps, such as offsets a past query skipped.

Topic names can be written without quotes, with `*` matching any run of characters and `?` one character:

```sql
SYNC demo-*, *-shop*, orders-20??;
```

Patterns are case-sensitive and match the topics the navigator lists. A pattern that matches no topic fails
the statement before anything is fetched. `SYNC` on its own syncs every cached topic the navigator still lists; `SYNC *` syncs every
listed topic, which can be a lot of data. Only one `SYNC` runs on a cache at a time: a second one, from any
program, fails at once instead of fetching the same records again. A query whose records are already cached
doesn't wait for a `SYNC`; one that must fetch waits its turn, as behind any other sync. `SYNC`
needs Kafka, so an offline connection refuses it.

## Examples

These queries use the [sample topics](sample-topics.md). Customers are compacted state; orders are append
history. Once that data is loaded, run them in your tool's SQL editor.

### Join customer state to orders

Each result row pairs an order's ID and amount with its customer's name and country. The customer's
schema-backed `key` column matches the order's `customer_id`.

```sql
-- Join orders with the current customer state
SELECT o.order_id, o.amount, c.name, c.country
FROM "orders" AS o JOIN "customers" AS c ON c.key = o.customer_id
WHERE o._timestamp >= unixepoch('now', '-1 day') * 1000;
```

### Inspect records

This returns the order fields beside their Kafka positions, restricted to one partition and offset range:

```sql
-- Inspect an offset range
SELECT _partition, _offset, order_id, status
FROM "orders"
WHERE _partition = 0 AND _offset BETWEEN 0 AND 10;
```

You can also read directly from the decoded JSON. This returns the status and key even without a typed
`status` column:

```sql
-- Read a JSON field without relying on a typed column
SELECT _value ->> '$.status' AS status, _key FROM "orders";
```

### Read the tail

The result contains up to five rows with the highest offsets across partitions. For newest records by time
instead, sort by `_timestamp` and bound the time range.

```sql
-- The five highest offsets, across partitions
SELECT order_id, status, _partition, _offset
FROM "orders" ORDER BY _offset DESC LIMIT 5;
```

### Compare plain JSON and schema-backed data

The plain JSON events produce one row per event type, with its count and average duration:

```sql
-- Columns inferred from plain JSON
SELECT type, count(*) AS n, avg(ms) AS avg_ms FROM "events" GROUP BY type;
```

The payment rows use JSON Schema; orders use Avro. They join through `order_id`, returning payment details
beside the order status:

```sql
-- Join JSON Schema and Avro records
SELECT p.payment_id, p.method, p.amount, o.status
FROM "payments" AS p JOIN "orders" AS o ON o.order_id = p.order_id;
```

### Inspect nested values and headers

This pattern is for your own topic with nested JSON and headers. It returns the address city for records
carrying a `trace-id` header:

```sql
SELECT o._value ->> '$.address.city' AS city
FROM "shipments" AS o, json_each(o._headers) AS h
WHERE h.value ->> '$.key' = 'trace-id';
```

## Window details

These rules determine how much Kafka data a query needs, before SQLite applies its filters:

| Query | Data needed |
|---|---|
| Append topic, no offset or timestamp bound | Everything Kafka still holds by default (`append.default.window=all`) |
| `_timestamp >= …` | From the first matching timestamp's offset in each partition to the captured end |
| `_partition = 0 AND _offset BETWEEN 0 AND 10` | Offsets 0–10 in partition 0, as far as Kafka still holds them |
| `_offset >= 0` | Everything Kafka still holds |
| Tail query, described below | Last `n` visible records per selected partition |
| Compacted topic, any `WHERE` | Full current state, then SQL filtering |
| `offline=true` | Cached ranges only; missing coverage fails the query |

The default `all` has no time or record cap and adds no default-window warning. To limit unbounded append
queries, set `append.default.window` to a duration and optionally change `append.default.window.max.records`.
That window adds a warning. See [configuration](configuration.md#the-drivers-own-settings).

Timestamp upper bounds do not limit fetching on CreateTime topics, where timestamps can be out of order.
SQLite still applies them to the result.

Conditions inside `OR` or `NOT`, functions applied to these columns, subqueries and payload fields derive no
bound: without another `_offset` or `_timestamp` bound, the configured window applies. A `_partition` bound
alone doesn't switch the window off.

A tail query, `ORDER BY _offset DESC LIMIT n` on one append topic with no other `WHERE` condition than
`_partition`, fetches the last `n` records of each partition instead of the default window. Its result is the
`n` highest offsets across those partitions.

## Column type details

These rules explain why a column may change type, disappear, or remain JSON as different records are cached.

- Schemas come only from cached records, without subject or latest-version lookups. Topics with several named
  record types get only the metadata and JSON columns.
- Plain JSON fields are sampled from the newest 1,000 cached records, with `key_` prefixes for fields in
  object keys. Conflicting types become text, with a warning. JDBC typed getters reject values that do not fit
  the inferred type with SQLState `22018`; `getString()` keeps the text.
- A field named like a metadata column gets a numeric suffix (`_1`, `_2`, …).
- When schema versions disagree on a field's type, a numeric type that safely holds both versions keeps the
  column typed. Other conflicts expose the original JSON value as text and attach a warning. Fields absent
  from a row are NULL.

### Protobuf

Proto3 scalar fields without explicit presence cannot distinguish an unset field from its default. They
therefore read as `0`, `false`, `""` or the first enum value, rather than NULL. Empty lists and maps stay
empty.
Unset `optional` fields, message fields and `oneof` members are NULL.

Signed 64-bit integers are `BIGINT` columns, though `_value` holds them as JSON strings. `uint64`/`fixed64`
stay exact `VARCHAR` text.

Well-known messages follow their JSON shape: strings for Timestamp/Duration/FieldMask, the wrapped primitive
for wrappers (`UInt64Value` stays text), and JSON for Struct/Value/ListValue/Any/Empty. At the top level they
have no columns for their internal encoding fields. Packed `Any` types must be in the record's schema or its
resolved references.

### Decimals

An Avro decimal's column is SQLite `NUMERIC`, so queries need no cast, but it is exact only to about 15
significant digits. The exact value stays as text in `_value`; read it with `_value ->> '$.amount'`.

## Table name details

SQLite reserves some names and treats letter case differently from Kafka. Use the names the navigator shows:

- A topic starting with `sqlite_` gets `sqlite-` instead.
- A second topic differing only in letter case gets a `$2` suffix, such as `"Orders$2"`.

## What SQL is refused

The driver reads each query with a SQL parser (JSqlParser) first, to find its topics and bounds, so a few
SQLite forms it can't parse are refused:

- `GLOB`;
- `BETWEEN … IS TRUE` or `BETWEEN … IS NULL` without parentheses around the `BETWEEN` expression.

Schema-qualified columns such as `main."orders"._offset` are unsupported. Use `"orders"._offset` or a table
alias instead.
