
import json
import math
import os
from kafka import KafkaConsumer

TOPIC = "intern-orders-vignesh"
TOTALS_FILE = "order_totals.json"

# Load previous cumulative totals, or start at zero.
if os.path.exists(TOTALS_FILE):
    with open(TOTALS_FILE, "r") as file:
        totals = json.load(file)
else:
    totals = {
        "processed_messages": 0,
        "valid": 0,
        "invalid": 0,
        "total_value": 0
    }

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="vignesh-order-processing-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=10000
)

try:
    for message in consumer:
        try:
            order = json.loads(message.value.decode("utf-8"))

            if not isinstance(order, dict):
                raise ValueError("Order must be a JSON object")

            order_id = order.get("order_id")
            product = order.get("product")
            quantity = order.get("quantity")
            price = order.get("unit_price")

            if not isinstance(order_id, str) or not order_id.strip():
                raise ValueError("Missing or invalid order_id")

            if not isinstance(product, str) or not product.strip():
                raise ValueError("Missing or invalid product")

            if type(quantity) is not int or quantity <= 0:
                raise ValueError("Quantity must be a positive integer")

            if type(price) not in (int, float):
                raise ValueError("Unit price must be numeric")

            if not math.isfinite(price) or price < 0:
                raise ValueError("Unit price must be finite and non-negative")

            order_total = quantity * price
            totals["valid"] += 1
            totals["total_value"] += order_total

            key = message.key.decode("utf-8") if message.key else None

            print("\nValid order")
            print("Kafka key:", key)
            print("Order ID:", order_id)
            print("Product:", product)
            print("Quantity:", quantity)
            print("Unit price: Rs.", price)
            print("Order total: Rs.", order_total)
            print("Partition:", message.partition, "Offset:", message.offset)

        except (ValueError, TypeError, KeyError, UnicodeDecodeError) as error:
            totals["invalid"] += 1
            print("\nInvalid order")
            print("Reason:", error)
            print("Raw message:", message.value)

        totals["processed_messages"] += 1

        # Save progress after each message.
        with open(TOTALS_FILE, "w") as file:
            json.dump(totals, file, indent=2)

        # Commit this message's offset after saving its totals.
        consumer.commit()

finally:
    consumer.close()

print("\n--- CUMULATIVE FINAL SUMMARY ---")
print("Total orders processed:", totals["processed_messages"])
print("Valid orders:", totals["valid"])
print("Invalid orders:", totals["invalid"])
print("Total value of valid orders: Rs.", totals["total_value"])
