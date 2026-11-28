# Record transforms

Keep chosen fields out of the local cache and query results: remove phone numbers or hash email addresses
so they can still join. **Drop and mask settings need no Java code.** Use a Java class for custom changes,
such as redacting free text or deriving an age band. To reshape data for a query, such as casting a field
or converting a timestamp, use SQL instead; see [When SQL is enough](#when-sql-is-enough).

The cache is the driver's local SQLite copy of fetched Kafka records. Transforms run after decoding and
before storage. The JSON columns `_key` and `_value`, headers and schema-derived columns all show the
transformed data. Offline queries read that stored copy; transforms do not run again.

Transforms change records only. The schemas fetched from Schema Registry are cached unchanged, including
field names, documentation and default values, so a schema that itself contains sensitive text, such as a
real address as a field's default, is copied into the cache as it is.

This is not access control. Original data still crosses the network to your machine, and anyone who can
change settings can remove a transform. Transforms do not erase earlier caches or exports. See
[cache security](configuration.md#cache-security) for protecting those copies.

## Use drop and mask without Java

**Drop** removes a field. **Mask** replaces it with a hash, a repeatable string calculated with your secret.
Matching values can still join or group without showing the originals. Every transformed topic needs
`mask.secret`, even if it only drops fields: the driver also hashes its stored key identity.

Add a policy to your [connection settings file](configuration.md#a-settings-file), then reconnect. Keep the
secret outside the URL and use your own random value. This example assumes your `customers` records have
the named fields; it does not add them to the quick start's sample data:

```properties
mask.secret=replace-with-your-own-random-secret

topic.customers.drop=phone,birth_date,address.street
topic.customers.mask=_key,email,address.postcode
topic.customers.drop.headers=authorization
topic.customers.mask.headers=client-ip
```

Paths such as `email` and `address.street` start in the value. Use `_key.email` for a key field, `_key` for
the entire key, or `_value` for the entire value. There are no wildcards or array indexes; drop or mask a
whole list. Absent paths are ignored. Header names are exact and case-sensitive, including duplicate names.

Query the topic to see what was stored:

```sql
SELECT _key, _value, _headers FROM "customers";
```

The key, email and postcode are hashes. Phone, birth date and street are absent from the JSON. The
`authorization` header is gone, and `client-ip` holds the same kind of 64-character hash text as the masked
fields:

```json
[{"key": "client-ip", "value": "ab90ff1c3e61b1d7256c8e54e840cfda8213e07cab9a5ff24486f752d76f682d"}]
```

For a ready-to-run example, use the demo's [privacy topics](../demo/README.md#privacy-topics).

## What queries see

`drop` removes a field from the record, not just its value: it disappears from `_value` or `_key`.
Whether a column remains depends on the topic:

- On a schema-backed topic (Avro, JSON Schema or Protobuf), columns come from the schema, so a dropped
  field's column stays and reads SQL NULL. Queries that name it keep working.
- On a plain JSON topic, columns are inferred from cached records, so a field dropped from every record
  has no column.

Masked fields have VARCHAR columns. Other schema columns keep their types: if a Java class changes a value
so it no longer fits, its typed column is NULL and the changed value remains in the JSON. See
[SQL columns](sql.md#columns) for the ordinary column rules.

On a transformed topic, a decoding or transform failure leaves a row with `_partition`, `_offset`,
`_timestamp` and `_decode_error`, but no key, value or headers. Driver and decoder failures use fixed
descriptions without payload details. A Java class's exception message is retained: do not put private data
in it or in logs. Integers outside the signed 64-bit range (`Long` in Java) fail before any class is called.

A compacted topic keeps current state per key per partition. A Kafka tombstone, a record with a null value,
still deletes its key even if decoding fails. The stored key identity is a hash of the original raw bytes,
so changing or dropping the visible key does not change which records replace or delete one another.

## How masking works

Masking uses HMAC-SHA256 and the secret, producing 64 lowercase hexadecimal characters. Its input is canonical
JSON: object keys are sorted at every level, strings include their quotes, and bytes use JSON's base64
representation. Equal canonical JSON text and the same secret give the same hash across topics and formats.
Protect the secret: without it, someone cannot test candidate values against the hashes.

Avro long `42` and Protobuf int64 `"42"` hash differently: one is a JSON number, the other a JSON string.
Normalise them in a Java transform before masking if they need to join.

## Changing a transform

The cache records a **fingerprint** of the policy, class version and secret-derived identity, never the
secret itself. Changing the settings, the class's `version()` or the secret rebuilds that topic on its next
online query. Removing transforms rebuilds it too.

An offline connection must keep the same policy, secret and any required class. It refuses a mismatched
fingerprint; reconnect online to rebuild first. Connections and processes sharing a cache must agree on
these choices. Rotating the secret gives new hashes that no longer match old exports or saved results.

## Writing a Java transform

Use a class when settings cannot express the change you need: redacting free text, deriving a field,
leaving some records out. The class runs first; the topic's `drop` and `mask` settings then run on what it
returns, so the class can still read a field that a setting removes. The demo's
[ShopPrivacy](../demo/transforms/demo/ShopPrivacy.java) is a complete example: it derives an age band from a
birth date and removes email addresses from free text.

A transform implements one method, `apply`, and may override `version`:

```java
public interface RecordTransform {
    Record apply(Record record) throws Exception;   // the changed record, or null to leave it out
    default String version() { return "1"; }        // change it when apply's output changes
}
```

Save this Java 21 class as `src/com/example/CustomerPrivacy.java`. It needs only the JDK and the driver JAR:

```java
package com.example;

import io.github.ifnesi.kafcacheql.transform.RecordTransform;
import java.util.Map;
import java.util.regex.Pattern;

public final class CustomerPrivacy implements RecordTransform {
    private static final Pattern CARD = Pattern.compile("\\b\\d{13,19}\\b");

    @Override
    public Record apply(Record record) {
        // Tombstones (null values) and non-object values pass through unchanged.
        if (!(record.value() instanceof Map<?, ?> raw)) return record;
        @SuppressWarnings("unchecked") var value = (Map<String, Object>) raw;

        if ("test".equals(value.get("source"))) return null;          // leave test records out of the cache
        if (value.get("notes") instanceof String notes) {
            value.put("notes", CARD.matcher(notes).replaceAll("[card removed]"));
        }
        record.headers(record.headers().stream()
                .filter(header -> !header.name().startsWith("x-debug-"))
                .toList());
        return record;
    }

    @Override
    public String version() { return "1"; }
}
```

### When the class runs

- **One instance per cache.** The driver creates the class once, with its public no-argument constructor,
  when a connection to the cache opens, and calls `version()` then. Topics that name the same class share
  that instance; use `record.topic()` if they need different logic.
- **Once per record, when it is fetched.** `apply` runs on each record as it is copied from Kafka into the
  cache, never at query time. Queries, including offline ones, read the stored result. A record is
  transformed again whenever it is fetched again: after it was evicted from the cache, or when the topic
  rebuilds (see [Changing a transform](#changing-a-transform)).
- **Many threads at once.** Several decoding threads call the same instance concurrently. Keep no mutable
  fields; constants and thread-safe objects such as a compiled `Pattern` are fine.
- **Only on records that decoded.** If the key or value fails to decode, `apply` is not called and the row
  keeps only its position and error (see [What queries see](#what-queries-see)).

### What `apply` receives

A `RecordTransform.Record` holds one decoded record:

| Method | Type | Notes |
|---|---|---|
| `topic()`, `partition()`, `offset()`, `timestamp()` | `String`, `int`, `long`, `long` | Read-only. The timestamp is Kafka's, in epoch milliseconds |
| `keySchemaId()`, `valueSchemaId()` | `String` | Read-only. The schema ID or GUID the record names, or `null` without a schema |
| `key()`, `value()` | `Object` | The decoded key and value; `null` for a null key or a tombstone |
| `headers()` | `List<Header>` | Every header, in order, duplicates included |

The key and value are the same JSON that `_key` and `_value` show for an untransformed topic, as plain Java
objects:

| JSON | Java type |
|---|---|
| object | `Map<String, Object>` (mutable, field order kept) |
| array | `List<Object>` (mutable) |
| string | `String` |
| integer | `Long` |
| other number | `Double` |
| `true` / `false` | `Boolean` |
| `null` | `null` |

So the decoder's JSON rules apply. Protobuf 64-bit integers and Avro decimals arrive as `String`, and
bytes arrive as base64 `String`, never `byte[]`. A key in `string` format is a `String`. See
[SQL columns](sql.md#columns) for how each format maps to JSON.

Each `RecordTransform.Header` has `name()` and `value()`. The value is the raw `byte[]`, or `null`.

### What `apply` returns

| Return | Result |
|---|---|
| The same record, changed or not | Stored after the topic's `drop` and `mask` settings run |
| A new `Record` | The same, but its topic, partition, offset, timestamp and schema IDs must equal the input's |
| `null` | Nothing is stored for this record. Its offset still counts as read, so it is not fetched again |

Change the payload with `record.key(...)`, `record.value(...)` and `record.headers(...)`, or by editing the
maps and lists you were given. Write values only of the types in the table above; `byte[]` is also accepted
and is stored as base64. Set headers to a new list of `new RecordTransform.Header(name, bytes)`, or to an
empty list to remove them all.

How changes show in queries:

- `_key`, `_value` and `_headers` show exactly what was stored.
- Typed columns of a schema-backed topic keep the schema's columns and types. A field you add that is not
  in the schema appears only in `_value`: read it with `_value ->> '$.field'`. A value that no longer fits
  its column's type reads NULL there and stays as returned in `_value`.
- Plain JSON topics infer their columns from cached data, so added fields become columns.

### Failures

- **The class throws.** The row keeps `_partition`, `_offset`, `_timestamp` and `_decode_error`, which
  reads `Record transform: ` followed by your exception's message. Key, value and headers are not stored.
  Other records continue. Keep private data out of the message: it is stored in the cache.

  This includes Java errors, not just exceptions. A missing dependency JAR, for example, gives an error
  row naming the missing class for each record that needs it, instead of failing the connection. Only
  errors that mean the JVM itself is in trouble, such as `OutOfMemoryError`, are not turned into rows: the
  query fails.
- **The output is invalid.** A changed identity field, or a value that cannot be stored, gives the same kind
  of row with a fixed message and no details.
- **The class cannot be used.** A missing class, a class that does not implement `RecordTransform`, a
  missing public no-argument constructor, a constructor that throws, or a `version()` that throws or
  returns `null` fails the connection. The driver never runs without a transform you configured.

To find the records that failed, and why:

```sql
SELECT _partition, _offset, _decode_error FROM "customer-events" WHERE _decode_error IS NOT NULL;
```

### Caveats

- **Tombstones go through `apply` too**, with a `null` value. Return them, changed or not, to keep
  deletions working. Returning `null` drops the deletion, so a compacted topic keeps the deleted key.
- **Skipping on a compacted topic keeps the older value.** If you return `null` for a key's newest record,
  the previous record for that key stays current.
- **Setting the value to `null` does not delete.** Whether a record deletes its key depends only on the
  original Kafka record. Likewise, changing or dropping the key does not change which records replace one
  another: the driver tracks keys by a hash of the original key bytes.
- **`mask` hashes what the class returned.** If the same value has different JSON in two topics, such as
  Avro long `42` and Protobuf int64 `"42"`, normalise it in the class so the masked values join; see
  [How masking works](#how-masking-works).
- **The class sees the original data.** It runs inside your tool with every field before drop and mask.
  Load only code you trust.

### Best practices

- **Prefer settings.** Use `drop` and `mask` where they suffice; keep the class for what they cannot do.
- **Be deterministic.** Base output only on the record: use `record.timestamp()`, not the current time, and
  no random values. A refetch, a rebuild or another process sharing the cache must produce the same result.
- **Stay fast and local.** `apply` runs on every fetched record, while a query waits. Do no network calls,
  file access or blocking.
- **Check types before casting.** Use `instanceof` patterns, as above. A value can be missing, `null` or of
  another type in some records, and a `ClassCastException` turns the record into an error row.
- **Bump `version()`** whenever the output changes, so existing cached records are transformed again.
  Otherwise old and new rows keep different output. Renaming the class rebuilds the topic too.
- **Don't hash values yourself.** Use the `mask` setting: it is keyed with `mask.secret`. A plain hash
  such as SHA-256 of an email address can be reversed by hashing a list of candidate addresses.
- **Keep dependencies few.** The JDK and the driver's `RecordTransform` interface are enough for most
  classes. The driver's bundled libraries are renamed internals, not an API: don't use them. Put any
  library you need on the same classpath as the driver.
- **Keep data out of logs and messages.** Do not log record contents or put them in exception messages.
- **Don't keep the record.** Do not store the record, or its maps and lists, after `apply` returns.
- **Test it like any Java class.** `Record` has a public constructor, so a unit test can call `apply`
  directly, as below.

```java
var record = new RecordTransform.Record("customer-events", 0, 42L, 1_700_000_000_000L, null, null,
        "c01", new HashMap<>(Map.of("notes", "card 4111111111111111")), new ArrayList<>());
var result = new CustomerPrivacy().apply(record);
assertEquals("card [card removed]", ((Map<?, ?>) result.value()).get("notes"));
```

### Build and load the class

Use a driver built from the current source, as described in [Development](../README.md#development). Compile
against its stable copy and package your class without copying the driver's interface into your JAR:

```sh
javac --release 21 -cp ~/.kafcacheql/kafcacheql-jdbc.jar -d classes src/com/example/CustomerPrivacy.java
jar --create --file customer-privacy.jar -C classes .
```

Add `customer-privacy.jar` beside the driver in your tool's library list; see
[tool setup](tools.md#tool-setup).
In a Java application, put both JARs on the same application classpath. Add any dependencies of your class
there too. The driver's class loader must be able to load the class.

Select it in the connection settings, alongside any built-in policy and `mask.secret`:

```properties
topic.customer-events.transform=com.example.CustomerPrivacy
```

Close and reopen the tool after replacing a JAR. After reconnecting and querying `customer-events`, card
numbers in `notes` read `[card removed]`, records with `"source": "test"` are absent, and no `x-debug-`
headers remain.

## When SQL is enough

Transforms exist to keep data out of the cache. Reshaping data for a query needs no transform: SQL does it
when you query, and the cache keeps the full record. If you know Kafka Connect's single message transforms
(SMTs), this is where each one's job is done:

| Connect SMT | In KafCacheQL |
|---|---|
| `MaskField`, `ReplaceField` (exclude), `DropHeaders` | `mask`, `drop` and `drop.headers` settings |
| `Filter` | A Java transform that returns `null` keeps records out of the cache; `WHERE` hides them from one query |
| `Cast` | `CAST(_value ->> '$.amount' AS REAL)` |
| `TimestampConverter` | `datetime(_timestamp / 1000, 'unixepoch')`; `unixepoch(_value ->> '$.created')` for ISO text |
| `Flatten`, `ExtractField` | `_value ->> '$.address.city'` |
| `ReplaceField` (rename) | `_value ->> '$.order_id' AS id` |
| `InsertField` (topic, partition, offset, timestamp) | Already columns: `_partition`, `_offset`, `_timestamp` |
| `ValueToKey`, `HeaderFrom`, `HoistField` | Not needed: `_key`, `_value` and `_headers` are on the same row |
| `TombstoneHandler` | Compacted topics apply tombstones as deletes; on append topics, `WHERE _value IS NOT NULL` |
| `RegexRouter`, `TimestampRouter`, `ExtractTopic` | Not applicable: a record's topic, partition and offset never change |

Two more patterns that need no transform, a header read as text and one row per array element:

```sql
SELECT h.value ->> '$.value' AS trace_id
FROM "orders" AS o, json_each(o._headers) AS h
WHERE h.value ->> '$.key' = 'trace-id';

SELECT o._offset, i.value ->> '$.sku' AS sku
FROM "orders" AS o, json_each(o._value, '$.items') AS i;
```

## Settings reference

All per-topic settings are optional. Any enabled transform requires the connection's secret.

| Setting | Effect |
|---|---|
| `mask.secret` | Secret for hashes; required even for drop-only policies or a Java class. Never stored in the cache; redacted like credentials |
| `topic.<name>.drop` | Comma-separated paths to remove |
| `topic.<name>.mask` | Comma-separated paths to replace with keyed hashes |
| `topic.<name>.drop.headers` | Comma-separated header names to remove, including duplicates |
| `topic.<name>.mask.headers` | Comma-separated header names whose byte values become ASCII hash text |
| `topic.<name>.transform` | One class implementing `RecordTransform` per topic, with a public no-argument constructor |
