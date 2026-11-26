# Sample topics

A small shop dataset for trying joins, grouping and JSON queries. Start with customers and orders: customer
state joins to order history through customer ID. Add events and payments to compare plain JSON with
schema-backed records. The [SQL examples](sql.md#examples) show what you can ask of each topic.

Avro and JSON Schema records identify their schemas in Schema Registry, so the driver can expose fields as
columns. Plain JSON needs no registry; its columns are inferred from the records in the local cache, the
SQLite copy filled by your queries.

## Before you produce

Start the stack from the [quick start](../README.md#quick-start), then run the commands from the repository
root. Producers run inside the containers. Examples with keys use `key|value`; an Avro string key is JSON,
hence its quotes.

Use a fresh stack or unused topic names. Rerunning the producers appends more records, so aggregate counts
will change.

## Customers and orders

Customers are **compacted state**: the latest value per key per partition. Orders are **append history**:
individual events, even when keys repeat. Both use Avro with Schema Registry. The customer key becomes the SQL
`key` column, which joins to an order's `customer_id`.

The commands create one partition for customers and three for orders, then write the sample records:

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

After the first query, customers expose `key`, `name` and `country`; orders expose their ID, customer ID,
status and amount. The sample contains three customers and four orders, ready for the [customer-order
join](sql.md#join-customer-state-to-orders).

## Events

These plain JSON events need no Schema Registry. They describe clicks and views, with a page and duration in
milliseconds. The driver [infers their columns](sql.md#columns) when queried:

```sh
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic events --partitions 1
printf '%s\n' '{"type":"click","page":"/home","ms":120}' '{"type":"view","page":"/cart","ms":80}' \
  '{"type":"click","page":"/cart","ms":95}' |
docker compose exec -T broker kafka-console-producer --bootstrap-server broker:29092 --topic events
```

The result has `type`, `page` and `ms` columns. Group by `type` to compare event counts and average durations.

## Payments

These payments use JSON Schema with Schema Registry. Each record has a payment ID, order ID, amount and
method. The `order_id` field joins to the Avro orders above, showing that SQL joins work across formats:

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

The result exposes those four fields as columns. Join to orders to see each payment's method and amount beside
its order status, using the [SQL examples](sql.md#examples).
