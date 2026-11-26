# SQL reference

Queries are SQLite's SQL: joins, aggregations, window functions, CTEs and SQLite's JSON functions. Only
SELECT statements are accepted.

## What SQL is refused

The driver reads each query with a SQL parser (JSqlParser) first, to find its topics and bounds, so a few
SQLite forms it can't parse are refused:

- `GLOB`;
- `BETWEEN … IS TRUE` or `IS NULL` without brackets;
- a column written with the schema name in front (`main."orders"._offset`; use `"orders"._offset` or an
  alias).

## Tables

Each topic is a view named after the topic. Quote names with dots or dashes: `"payments.v1"`.

- A topic starting with `sqlite_`, a prefix SQLite reserves, gets `sqlite-` instead.
- A second topic differing only in letter case gets a `$2` suffix, `"Orders$2"`: use the name the navigator
  shows.

## Columns

Every view has the record's metadata:

| Column | Meaning |
|---|---|
| `_partition`, `_offset` | Where the record is in Kafka |
| `_timestamp` | Record timestamp, epoch milliseconds |
| `_key`, `_value` | Decoded key and value, as JSON |
| `_headers` | JSON array of `{"key": …, "value": …}`, values in base64 |
| `_key_schema_id`, `_value_schema_id` | The schema each record names |
| `_decode_error` | Why a record couldn't be decoded; its raw bytes are then kept in base64 |

Plus a column per top-level field:

- **Avro and JSON Schema:** of the schemas in the topic's cached records. A primitive key with a schema is
  also a `key` column.
- **Plain JSON:** of the top-level keys in its newest 1,000 cached records. A key whose type varies becomes a
  text column, with a warning; a value that doesn't fit its column's type fails the getter with SQLState
  `22018`.
- A key without a schema is in `_key` (`_key ->> '$'`).
- A field named like a metadata column gets a numeric suffix (`_1`, `_2`, …).

Nested values stay JSON; reach them with `->>`: `_value ->> '$.address.city'`.

**Decimals.** An Avro decimal's column is SQLite `NUMERIC`, so queries need no cast, but it's exact only to
about 15 significant digits. The exact value is kept as text in `_value` (`_value ->> '$.amount'`).

## What a query reads

The driver works out, per topic, which offsets a query needs from its `WHERE` clause on `_partition`,
`_offset` and `_timestamp`, and fetches only those not yet cached. The [README](../README.md#what-a-query-reads)
has the table. Conditions inside `OR` or `NOT`, functions applied to these columns, subqueries and payload
fields derive no bound: without another `_offset` or `_timestamp` bound, the default window applies, with a
warning. A `_partition` bound alone doesn't switch the default window off.

A tail query, `ORDER BY _offset DESC LIMIT n` on one append topic with no other `WHERE` condition than
`_partition`, fetches the last `n` records of each partition instead of the default window. Its result is the
`n` highest offsets across those partitions. Offsets are numbered per partition, so these aren't the topic's
`n` latest records: for those, sort by `_timestamp` and bound it.
