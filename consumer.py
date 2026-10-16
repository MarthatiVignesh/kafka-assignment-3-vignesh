
import json
import math
from kafka import KafkaConsumer

topic = "intern-orders-vignesh"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    group_id="vignesh-order-processing-group",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000
)

valid = 0
invalid = 0
total = 0

for message in consumer:
    try:
        # Convert message bytes into a Python dictionary
        order = json.loads(message.value.decode("utf-8"))

        # Check whether the message is a JSON object
        if not isinstance(order, dict):
            raise ValueError("Order must be a JSON object")

        # Check order ID
        order_id = order.get("order_id")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("Missing or invalid order_id")

        # Check product name
        product = order.get("product")
        if not isinstance(product, str) or not product.strip():
            raise ValueError("Missing or invalid product")

        # Check quantity
        quantity = order.get("quantity")
        if type(quantity) is not int or quantity <= 0:
            raise ValueError("Quantity must be a positive integer")

        # Check unit price
        price = order.get("unit_price")
        if type(price) not in (int, float):
            raise ValueError("Unit price must be numeric")

        if not math.isfinite(price) or price < 0:
            raise ValueError("Unit price must be finite and non-negative")

        # Calculate order total
        order_total = quantity * price
        valid += 1
        total += order_total

        # Display valid order
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
        invalid += 1
        print("\nInvalid order")
        print("Reason:", error)
        print("Raw message:", message.value)

consumer.close()

print("\n--- FINAL SUMMARY ---")
print("Valid orders:", valid)
print("Invalid orders:", invalid)
print("Total value of valid orders: Rs.", total)
