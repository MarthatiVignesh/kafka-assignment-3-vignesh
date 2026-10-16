
import json
from kafka import KafkaProducer

topic = "intern-orders-vignesh"

orders = [
    {"order_id": "O101", "product": "Mouse", "quantity": 2, "unit_price": 800},
    {"order_id": "O102", "product": "Keyboard", "quantity": 1, "unit_price": 1500},
    {"order_id": "O103", "product": "Notebook", "quantity": 3, "unit_price": 50},
    {"order_id": "O104", "product": "Monitor", "quantity": 1, "unit_price": 7500},
    {"order_id": "O105", "product": "USB Cable", "quantity": 4, "unit_price": 150},
    {"order_id": "O106", "product": "Headphones", "quantity": 2, "unit_price": 1200},
    {"order_id": "O107", "product": "Webcam", "quantity": 1, "unit_price": 2200},
    {"order_id": "O108", "product": "Laptop Stand", "quantity": 2, "unit_price": 900},
    {"order_id": "O109", "product": "Pen", "quantity": 5, "unit_price": 20},
    {"order_id": "O110", "product": "Keyboard", "quantity": 2, "unit_price": 1500},
    {"order_id": "O111", "product": "Mouse", "unit_price": 800},
    {"order_id": "O112", "product": "Monitor", "quantity": 1, "unit_price": "unknown"}
]

producer = KafkaProducer(bootstrap_servers="localhost:9092")

for order in orders:
    key = order["order_id"].encode("utf-8")
    value = json.dumps(order).encode("utf-8")

    producer.send(topic, key=key, value=value)
    print("Sent:", order)

producer.flush()
producer.close()

print("All orders sent successfully!")
