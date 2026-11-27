"""A completed web-store order per tick; business times compress its delivery lifecycle."""
import json
import os
import signal
import time
import uuid

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

FIELDS = {
    "customer-profiles": "customer_id:string email:string phone:string birth_date:string address:string",
    "customer-notes": "note_id:string email:string birth_date:string age_band:string text:string",
    "customers": "customer_id:string name:string region:string",
    "products": "product_id:string name:string category:string price_cents:long",
    "stock": "product_id:string quantity:long updated_at:long",
    "orders": "order_id:string customer_id:string ordered_at:long total_cents:long status:string",
    "order-items": "item_id:string order_id:string product_id:string quantity:long unit_price_cents:long",
    "payments": "payment_id:string order_id:string amount_cents:long method:string paid_at:long",
    "shipments": "shipment_id:string order_id:string region:string promised_at:long delivered_at:long",
    "delivery-events": "event_id:string shipment_id:string order_id:string status:string event_at:long",
    "returns": "return_id:string order_id:string product_id:string quantity:long reason:string",
    "reviews": "review_id:string order_id:string customer_id:string product_id:string rating:long",
}
PRODUCTS = [("Coffee", "Pantry", 950), ("Tea", "Pantry", 650), ("Mug", "Kitchen", 1200),
            ("Kettle", "Kitchen", 4500), ("Notebook", "Stationery", 800), ("Pen", "Stationery", 300),
            ("Lamp", "Home", 3500), ("Cushion", "Home", 2200)]
REGIONS = ["North", "South", "East", "West"]


def main():
    rate = float(os.environ.get("ORDERS_PER_SECOND", "2"))
    if not 0 < rate < float("inf"):
        raise ValueError("ORDERS_PER_SECOND must be positive and finite")
    bootstrap = os.environ.get("BOOTSTRAP_SERVERS", "broker:29092")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [NewTopic("shop-" + name, num_partitions=3, replication_factor=1,
                      config={"cleanup.policy": "compact" if name in ("customers", "products", "stock", "customer-profiles") else "delete"})
              for name in FIELDS]
    for future in admin.create_topics(topics).values():
        try:
            future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    registry = SchemaRegistryClient({"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")})
    serializers = {}
    for name, fields in FIELDS.items():
        schema = {"type": "record", "name": name.replace("-", "_") + "_value", "namespace": "demo.store",
                  "fields": [dict(zip(("name", "type"), field.split(":"))) for field in fields.split()]}
        if name == "customer-profiles":
            schema["fields"][-1]["type"] = {"type": "record", "name": "Address", "fields": [
                {"name": "street", "type": "string"}, {"name": "postcode", "type": "string"}]}
        serializers[name] = AvroSerializer(registry, json.dumps(schema))
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "linger.ms": 5})
    failures = []

    def delivered(error, message):
        if error is not None:
            failures.append(error)

    def send(entity, key, timestamp, **value):
        topic = "shop-" + entity
        producer.produce(topic, key=key.encode(), value=serializers[entity](value, SerializationContext(topic, MessageField.VALUE)),
                         timestamp=timestamp, on_delivery=delivered,
                         headers=[("client-ip", b"192.0.2.15"), ("authorization", b"demo-private-token")]
                         if entity in ("customer-profiles", "customer-notes") else None)

    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    now = int(time.time() * 1000)
    for n in range(20):
        cid = f"c{n + 1:02}"
        send("customers", cid, now, customer_id=cid, name=f"Customer {n + 1:02}", region=REGIONS[n % 4])
        email = f"customer{n + 1:02}@example.test"
        send("customer-profiles", email, now, customer_id=cid, email=email, phone=f"+1-555-010{n % 10}",
             birth_date=f"{1970 + n}-06-15", address={"street": f"{n + 1} Example Street", "postcode": "DEMO 123"})
    quantities = [1000] * len(PRODUCTS)
    for n, (name, category, price) in enumerate(PRODUCTS):
        pid = f"p{n + 1:02}"
        send("products", pid, now, product_id=pid, name=name, category=category, price_cents=price)
        send("stock", pid, now, product_id=pid, quantity=quantities[n], updated_at=now)
    producer.flush()
    run = uuid.uuid4().hex[:8]
    sequence = 0
    tick = time.monotonic()
    print(f"Producing twelve shop-* topics at {rate:g} orders/s; run={run}", flush=True)
    try:
        while running:
            now = int(time.time() * 1000)
            ordered = now - 120_000
            oid = f"o-{run}-{sequence:08}"
            cid = f"c{sequence % 20 + 1:02}"
            email = f"customer{sequence % 20 + 1:02}@example.test"
            send("customer-notes", email, now, note_id="note-" + oid, email=email,
                 birth_date=f"{1970 + sequence % 20}-06-15", age_band="",
                 text=f"Please contact {email} about delivery")
            indices = [sequence % 8, (sequence + 3) % 8]
            total = sum(PRODUCTS[n][2] * (n % 2 + 1) for n in indices)
            send("orders", oid, ordered, order_id=oid, customer_id=cid, ordered_at=ordered, total_cents=total, status="paid")
            for line, n in enumerate(indices):
                pid, quantity = f"p{n + 1:02}", n % 2 + 1
                item = f"{oid}-{line}"
                send("order-items", item, ordered, item_id=item, order_id=oid, product_id=pid,
                     quantity=quantity, unit_price_cents=PRODUCTS[n][2])
                quantities[n] = max(0, quantities[n] - quantity)
                send("stock", pid, now, product_id=pid, quantity=quantities[n], updated_at=now)
            payment, shipment = "pay-" + oid, "ship-" + oid
            send("payments", payment, ordered + 1000, payment_id=payment, order_id=oid, amount_cents=total,
                 method="card" if sequence % 2 else "wallet", paid_at=ordered + 1000)
            promised = now - 20_000
            arrived = promised + (20_000 if sequence % 3 == 0 else -10_000)
            send("shipments", shipment, arrived, shipment_id=shipment, order_id=oid,
                 region=REGIONS[sequence % 4], promised_at=promised, delivered_at=arrived)
            send("delivery-events", shipment, arrived, event_id=shipment, shipment_id=shipment,
                 order_id=oid, status="delivered", event_at=arrived)
            pid = f"p{indices[0] + 1:02}"
            if sequence % 10 == 0:
                rid = "ret-" + oid
                send("returns", rid, now, return_id=rid, order_id=oid, product_id=pid, quantity=1, reason="size")
            if sequence % 3 == 0:
                rid = "rev-" + oid
                send("reviews", rid, now, review_id=rid, order_id=oid, customer_id=cid, product_id=pid, rating=sequence % 5 + 1)
            producer.poll(0)
            if failures:
                raise KafkaException(failures[0])
            sequence += 1
            if sequence % 20 == 0:
                print(f"Completed {sequence} orders", flush=True)
            tick += 1 / rate
            time.sleep(max(0, tick - time.monotonic()))
    finally:
        remaining = producer.flush(10)
        if failures or remaining:
            raise RuntimeError(f"Delivery failed: {failures}; {remaining} records unacknowledged")


if __name__ == "__main__":
    main()
