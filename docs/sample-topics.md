# Sample topics

Topics for trying KafCacheQL on the Docker stack in this repository (`docker compose up -d broker
schema-registry`). The console producers run inside the containers; each line is `key|value`, and an Avro
key is JSON too, hence the quotes. The [README](../README.md#sql-examples)'s SQL examples run on this data.

## `customers` and `orders` (Avro, Schema Registry)

A compacted `customers` topic and an append `orders` topic with three partitions:

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

## `events` (plain JSON, no Schema Registry)

Its columns come from the top-level keys of its newest 1,000 cached records:

```sh
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic events --partitions 1
printf '%s\n' '{"type":"click","page":"/home","ms":120}' '{"type":"view","page":"/cart","ms":80}' \
  '{"type":"click","page":"/cart","ms":95}' |
docker compose exec -T broker kafka-console-producer --bootstrap-server broker:29092 --topic events
```

## `payments` (JSON Schema, Schema Registry)

Like an Avro topic: a column per property of the schemas in its cached records:

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
